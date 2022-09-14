from typing import List, Union
from .code import Code
from .encounter import Encounter
from model.node import Node
from .neo4j_connection import Neo4jConnection
from uuid import UUID, uuid4

class StudyEpoch(Node):
  uuid: str
  studyEpochName: str
  studyEpochDesc: str
  previousStudyEpochId: Union[UUID, None] = None
  nextStudyEpochId: Union[UUID, None] = None
  studyEpochType: Union[Code, UUID] = None
  encounters: Union[List[Encounter], List[UUID]] = []

  def add_encounter(self, uuid):
    db = Neo4jConnection()
    with db.session() as session:
      return session.execute_write(self._add_encounter, str(self.uuid), uuid)

  @staticmethod
  def _add_encounter(tx, epoch_uuid, encounter_uuid):
    query = (
      "MATCH (ep:StudyEpoch { uuid: $uuid1 }), (e:Encounter { uuid: $uuid2 })"
      "MERGE (ep)-[:ENCOUNTER]->(e)"
    )
    result = tx.run(query, uuid1=epoch_uuid, uuid2=encounter_uuid)
    return encounter_uuid

