from typing import List, Union, Literal
from .base_node import *
from .procedure import Procedure
from .biomedical_concept import BiomedicalConcept
from .biomedical_concept_surrogate import BiomedicalConceptSurrogate
from .biomedical_concept_category import BiomedicalConceptCategory
from .schedule_timeline import ScheduleTimeline
from d4kms_service import Neo4jConnection

class Activity(NodeNameLabelDesc):
  previous: Union[NodeId, None] = None
  next: Union[NodeId, None] = None
  definedProcedures: List[Procedure] = []
  biomedicalConcepts: List[BiomedicalConcept] = []
  bcCategories: List[BiomedicalConceptCategory] = []
  bcSurrogates: List[BiomedicalConceptSurrogate] = []
  timeline: Union[ScheduleTimeline, None] = None
  instanceType: Literal['Activity']

# class ActivityIn(BaseModel):
#   name: str
#   description: str
  
# class Activity(BaseNode):
#   uuid: Union[UUID, None] = None
#   activityName: str
#   activityDesc: str
#   previousActivityId: Union[UUID, None] = None
#   nextActivityId: Union[UUID, None] = None
#   definedProcedures: Union[List[Procedure], List[UUID]] = []
#   studyDataCollection: Union[List[StudyData], List[UUID]] = []

#   @classmethod
#   def create(cls, uuid, name, description):
#     db = Neo4jConnection()
#     with db.session() as session:
#       if session.execute_read(cls._any, uuid):
#         if not session.execute_read(cls._exists, uuid, name):
#           return session.execute_write(cls._create_activity, uuid, name, description)
#         else:
#           return None
#       else:
#         return session.execute_write(cls._create_first_activity, uuid, name, description)

#   def add_study_data(self, name, description, link):
#     db = Neo4jConnection()
#     with db.session() as session:
#       return session.execute_write(self._create_study_data, str(self.uuid), name, description, link)

  def children(self):
    db = Neo4jConnection()
    with db.session() as session:
      result = session.execute_read(self._children, str(self.uuid))
      #print(f"AC: {result}")
      return result

#   @staticmethod
#   def _create_activity(tx, uuid, name, description):
#       query = (
#         "MATCH (sd:StudyDesign { uuid: $uuid1 })-[:STUDY_ACTIVITY]->(a)"
#         "WHERE NOT (a)-[:NEXT_ACTIVITY]->()"
#         "CREATE (a1:Activity { activityName: $name, activityDesc: $desc, uuid: $uuid2 })"
#         "CREATE (a)-[:NEXT_ACTIVITY]->(a1)"
#         "CREATE (a1)-[:PREVIOUS_ACTIVITY]->(a)"
#         "CREATE (sd)-[:STUDY_ACTIVITY]->(a1)"
#         "RETURN a1.uuid as uuid"
#       )
#       result = tx.run(query, name=name, desc=description, uuid1=uuid, uuid2=str(uuid4()))
# #      try:
#       for row in result:
#         return row["uuid"]
#       return None
# #      except ServiceUnavailable as exception:
# #        logging.error("{query} raised an error: \n {exception}".format(
# #          query=query, exception=exception))
# #        raise

#   @staticmethod
#   def _create_first_activity(tx, uuid, name, description):
#     query = (
#       "MATCH (sd:StudyDesign { uuid: $uuid1 })"
#       "CREATE (a:Activity { activityName: $name, activityDesc: $desc, uuid: $uuid2 })"
#       "CREATE (sd)-[:STUDY_ACTIVITY]->(a)"
#       "RETURN a.uuid as uuid"
#     )
#     result = tx.run(query, name=name, desc=description, uuid1=uuid, uuid2=str(uuid4()))
#     for row in result:
#       return row["uuid"]
#     return None

#   @staticmethod
#   def _create_study_data(tx, uuid, name, description, link):
#     query = (
#       "MATCH (a:Activity { uuid: $uuid1 })"
#       "CREATE (sd:StudyData { studyDataName: $name, studyDataDesc: $desc, crfLink: $link, uuid: $uuid2 })"
#       "CREATE (a)-[:STUDY_DATA_COLLECTION]->(sd)"
#       "RETURN sd.uuid as uuid"
#     )
#     result = tx.run(query, name=name, desc=description, link=link, uuid1=uuid, uuid2=str(uuid4()))
#     for row in result:
#       return row["uuid"]
#     return None

  @staticmethod
  def _children(tx, uuid):
    query = """
      MATCH (a:Activity { uuid: $uuid1 })-[r:BIOMEDICAL_CONCEPT_REL|BC_SURROGATE_REL|DEFINED_PROCEDURES_REL|TIMELINE_REL]->(child)
      RETURN child.name as name, child.label as label, child.uuid as uuid, labels(child) as type
    """
    result = tx.run(query, uuid1=uuid)
    results = []
    for row in result:
      result = dict(row)
      result['type'] = result['type'][0]
      results.append(result)
    return results

#   @staticmethod
#   def _exists(tx, uuid, name):
#     query = (
#       "MATCH (sd:StudyDesign { uuid: $uuid })-[:STUDY_ACTIVITY]->(a:Activity { activityName: $name })"
#       "RETURN a.uuid as uuid"
#     )
#     result = tx.run(query, name=name, uuid=uuid)
#     if result.peek() == None:
#       return False
#     else:
#       return True

#   @staticmethod
#   def _any(tx, uuid):
#     query = (
#       "MATCH (sd:StudyDesign { uuid: $uuid })-[:STUDY_ACTIVITY]->(a:Activity)"
#       "RETURN a.uuid"
#     )
#     result = tx.run(query, uuid=uuid)
#     if result.peek() == None:
#       return False
#     else:
#       return True
