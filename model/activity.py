from typing import List, Union
from pydantic import BaseModel
from .procedure import Procedure
from .study_data import StudyData
from .neo4j_connection import Neo4jConnection
from uuid import UUID, uuid4

class ActivityIn(BaseModel):
  activityName: str
  activityDesc: str
  
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

  def create(cls, name, description):
    db = Neo4jConnection()
    with db.session() as session:
      if not session.execute_read(cls._exists, name, description):
        result = session.execute_write(cls._create_study, name, description)
        return result
      else:
        return None

  @staticmethod
  def _create_activity(tx, study_uuid, name, description):
      query = (
        "MATCH (n:Study {uuid: $uuid1})-[]->(sd:StudyDesign)-[]->(a:StudyActivity)"
        "WHERE NOT (a)-[:NEXT_ACTIVITY]->()"
        "CREATE (s:Activity { activityName: $name, activityDesc: $desc, uuid: $uuid1 })"
        "CREATE (a)-[:NEXT_ACTIVIY]->(s)"
        "CREATE (s)-[:PREVIOUS_ACTIVIY]->(a)"
      )
      result = tx.run(query, name=name, desc=description, uuid1=str(uuid4()))
#      try:
      for row in result:
        return row["uuid"]
      return None
#      except ServiceUnavailable as exception:
#        logging.error("{query} raised an error: \n {exception}".format(
#          query=query, exception=exception))
#        raise

