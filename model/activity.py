from typing import List, Union
from pydantic import BaseModel
from .procedure import Procedure
from .study_data import StudyData
from .neo4j_connection import Neo4jConnection
from uuid import UUID, uuid4

class ActivityIn(BaseModel):
  name: str
  description: str
  
class Activity(BaseModel):
  uuid: Union[UUID, None] = None
  activityName: str
  activityDesc: str
  previousActivityId: Union[UUID, None] = None
  nextActivityId: Union[UUID, None] = None
  definedProcedures: Union[List[Procedure], List[UUID]] = []
  studyDataCollection: Union[List[StudyData], List[UUID]] = []

  def __init__(self):
    self.uuid = None
    self.uri = None
    self.activityName = ""  
    self.activityDesc = None
    self.previousActivityId = None
    self.nextActivityId = None
    self.definedProcedures = []
    self.studyDataCollection = []

  @classmethod
  def create(cls, uuid, name, description):
    print("B")
    db = Neo4jConnection()
    with db.session() as session:
      print("C")
      if session.execute_read(cls._any, uuid):
        print("D")
        if not session.execute_read(cls._exists, name):
          print("E")
          return session.execute_write(cls._create_activity, uuid, name, description)
        else:
          print("F")
          return None
      else:
        print("G")
        return session.execute_write(cls._create_first_activity, uuid, name, description)

  @staticmethod
  def _create_activity(tx, uuid, name, description):
      query = (
        "MATCH (n:Study { uuid: $uuid })-[]->(sd:StudyDesign)-[]->(a:StudyActivity)"
        "WHERE NOT (a)-[:NEXT_ACTIVITY]->()"
        "CREATE (s:Activity { activityName: $name, activityDesc: $desc, uuid: $uuid2 })"
        "CREATE (a)-[:NEXT_ACTIVIY]->(s)"
        "CREATE (s)-[:PREVIOUS_ACTIVIY]->(a)"
        "CREATE (sd)-[:STUDY_ACTIVIY]->(a)"
        "RETURN s.uuid as uuid"
      )
      print(query)
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
  def _create_first_activity(tx, uuid, name, description):
      query = (
        "MATCH (n:Study { uuid: $uuid1 })-[]->(sd:StudyDesign)"
        "CREATE (s:Activity { activityName: $name, activityDesc: $desc, uuid: $uuid2 })"
        "CREATE (sd)-[:STUDY_ACTIVIY]->(a)"
        "RETURN s.uuid as uuid"
      )
      print(query)
      result = tx.run(query, name=name, desc=description, uuid1=uuid, uuid2=str(uuid4()))
      print(result.peek())
      for row in result:
        print("X", row["uuid"])
        return row["uuid"]
      return None

  @staticmethod
  def _exists(tx, uuid, name):
      query = (
        "MATCH (s:Study { uuid: $uuid })-[]->(sd:StudyDesign)-[]->(a:StudyActivity { name: $name }))"
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
        "OPTIONAL MATCH (s:Study { uuid: $uuid })-[]->(sd:StudyDesign)-[]->(a:StudyActivity)"
        "RETURN a IS NOT NULL AS any"
      )
      result = tx.run(query, uuid=uuid)
      print(result.peek()['any'])
      return result.peek()['any']
