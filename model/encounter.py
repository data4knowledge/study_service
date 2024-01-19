from typing import List, Union
from pydantic import BaseModel
from model.code import Code
from model.transition_rule import TransitionRule
from model.base_node import BaseNode
from uuid import UUID, uuid4
from d4kms_service import Neo4jConnection

class EncounterIn(BaseModel):
  name: str
  description: str

class EncounterLink(BaseModel):
  uuid: str

class Encounter(BaseNode):
  uuid: Union[UUID, None] = None
  uri: str = ""
  encounterName: str
  encounterDesc: str
  previousEncounterId: Union[UUID, None] = None
  nextEncounterId: Union[UUID, None] = None
  encounterType: Union[Code, UUID, None] = None
  encounterEnvironmentalSetting: Union[Code, UUID, None] = None
  encounterContactMode: Union[Code, UUID, None] = None
  transitionStartRule: Union[TransitionRule, UUID, None] = None
  transitionEndRule: Union[TransitionRule, UUID, None] = None

  @classmethod
  def create(cls, uuid, name, description):
    db = Neo4jConnection()
    with db.session() as session:
      if session.execute_read(cls._any, uuid):
        if not session.execute_read(cls._exists, uuid, name):
          return session.execute_write(cls._create_encounter, uuid, name, description)
        else:
          return None
      else:
        return session.execute_write(cls._create_first_encounter, uuid, name, description)

  @staticmethod
  def _create_encounter(tx, uuid, name, description):
      query = (
        "MATCH (sd:StudyDesign { uuid: $uuid1 })-[:STUDY_ENCOUNTER]->(e)"
        "WHERE NOT (e)-[:NEXT_ENCOUNTER]->()"
        "CREATE (e1:Encounter { encounterName: $name, encounterDesc: $desc, uuid: $uuid2 })"
        "CREATE (e)-[:NEXT_ENCOUNTER]->(e1)"
        "CREATE (e1)-[:PREVIOUS_ENCOUNTER]->(e)"
        "CREATE (sd)-[:STUDY_ENCOUNTER]->(e1)"
        "RETURN e1.uuid as uuid"
      )
      result = tx.run(query, name=name, desc=description, uuid1=uuid, uuid2=str(uuid4()))
#      try:
      for row in result:
        return row["uuid"]
      return None
#      except ServiceUnavailable as exception:
#        logging.error("{query} raised an error: \n {exception}".format(
#          query=query, exception=exception))
#        raise

  @staticmethod
  def _create_first_encounter(tx, uuid, name, description):
    query = (
      "MATCH (sd:StudyDesign { uuid: $uuid1 })"
      "CREATE (e:Encounter { encounterName: $name, encounterDesc: $desc, uuid: $uuid2 })"
      "CREATE (sd)-[:STUDY_ENCOUNTER]->(e)"
      "RETURN e.uuid as uuid"
    )
    result = tx.run(query, name=name, desc=description, uuid1=uuid, uuid2=str(uuid4()))
    for row in result:
      return row["uuid"]
    return None

  @staticmethod
  def _exists(tx, uuid, name):
    query = (
      "MATCH (ep:StudyDesign { uuid: $uuid })-[:STUDY_ENCOUNTER]->(e:Encounter { encounterName: $name })"
      "RETURN e.uuid as uuid"
    )
    result = tx.run(query, name=name, uuid=uuid)
    if result.peek() == None:
      return False
    else:
      return True

  @staticmethod
  def _any(tx, uuid):
    query = (
      "MATCH (sd:StudyDesign { uuid: $uuid })-[:STUDY_ENCOUNTER]->(e:Encounter)"
      "RETURN e.uuid"
    )
    result = tx.run(query, uuid=uuid)
    if result.peek() == None:
      return False
    else:
      return True
