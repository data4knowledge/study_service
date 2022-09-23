from pydantic import BaseModel
from typing import List, Union
from model.code import Code
from model.encounter import Encounter
from model.node import Node
from model.neo4j_connection import Neo4jConnection
from uuid import UUID, uuid4

class StudyEpochIn(BaseModel):
  name: str
  description: str
  
class StudyEpoch(Node):
  uuid: str
  studyEpochName: str
  studyEpochDesc: str
  previousStudyEpochId: Union[UUID, None] = None
  nextStudyEpochId: Union[UUID, None] = None
  studyEpochType: Union[Code, UUID] = None
  encounters: Union[List[Encounter], List[UUID]] = []

  @classmethod
  def create(cls, uuid, name, description):
    db = Neo4jConnection()
    with db.session() as session:
      print("A")
      print("B")
      if not session.execute_read(cls._exists, uuid, name):
        print("C")
        arms = session.execute_read(cls._arms, uuid)
        return session.execute_write(cls._create_epoch, uuid, name, description, arms)
      else:
        return None

  def add_encounter(self, uuid):
    db = Neo4jConnection()
    with db.session() as session:
      return session.execute_write(self._add_encounter, str(self.uuid), uuid)

  @staticmethod
  def _create_epoch(tx, uuid, name, description, arms):
      query = """
        MATCH (sd:StudyDesign { uuid: $uuid1 })-[:STUDY_CELL]->(c:StudyCell)-[:STUDY_EPOCH]->(a:StudyEpoch)
        WHERE NOT (a)-[:NEXT_EPOCH]->()
        CREATE (a1:StudyEpoch { studyEpochName: $name, studyEpochDesc: $desc, uuid: $uuid2 })
        CREATE (a)-[:NEXT_EPOCH]->(a1)
        CREATE (a1)-[:PREVIOUS_EPOCH]->(a)
        WITH sd, a1
        UNWIND $arms AS arm
          MATCH (sa:StudyArm { uuid: arm })
          CREATE (c1:StudyCell { uuid: $uuid3 })
          CREATE (sd)-[:STUDY_CELL]->(c1)
          CREATE (c1)-[:STUDY_ARM]->(sa)
          CREATE (c1)-[:STUDY_EPOCH]->(a1)
        RETURN a1.uuid as uuid
      """
      result = tx.run(query, name=name, desc=description, uuid1=uuid, uuid2=str(uuid4()), uuid3=str(uuid4()), arms=arms)
#      try:
      for row in result:
        return row["uuid"]
      return None
#      except ServiceUnavailable as exception:
#        logging.error("{query} raised an error: \n {exception}".format(
#          query=query, exception=exception))
#        raise

  @staticmethod
  def _arms(tx, uuid):
    query = (
      "MATCH (sd:StudyDesign { uuid: $uuid1 })-[:STUDY_CELL]->(c)-[:STUDY_ARM]->(a:StudyArm)"
      "RETURN DISTINCT a.uuid as uuid"
    )
    result = tx.run(query, uuid1=uuid)
    results = []
    for row in result:
      results.append(row["uuid"])
    print("ARMS:", results)
    return results

  @staticmethod
  def _exists(tx, uuid, name):
    query = (
      "MATCH (sd:StudyDesign { uuid: $uuid })-[:STUDY_CELL]->(c)-[:STUDY_EPOCH]->(a:StudyEpoch { epochName: $name })"
      "RETURN a.uuid as uuid"
    )
    result = tx.run(query, name=name, uuid=uuid)
    if result.peek() == None:
      return False
    else:
      return True

  @staticmethod
  def _any(tx, uuid):
    query = (
      "MATCH (sd:StudyDesign { uuid: $uuid })-[:STUDY_CELL]->(c)-[:STUDY_EPOCH]->(a:StudyEpoch)"
      "RETURN a.uuid"
    )
    result = tx.run(query, uuid=uuid)
    if result.peek() == None:
      return False
    else:
      return True

  @staticmethod
  def _add_encounter(tx, epoch_uuid, encounter_uuid):
    query = (
      "MATCH (ep:StudyEpoch { uuid: $uuid1 }), (e:Encounter { uuid: $uuid2 })"
      "MERGE (ep)-[:ENCOUNTER]->(e)"
    )
    result = tx.run(query, uuid1=epoch_uuid, uuid2=encounter_uuid)
    return encounter_uuid

