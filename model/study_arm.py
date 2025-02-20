import traceback
import logging
from typing import List, Literal
from .base_node import *
from .code import Code
from uuid import uuid4

# class StudyArmIn(BaseModel):
#   name: str
#   description: str

class StudyArm(NodeNameLabelDesc):
  # type: Code
  type: str
  dataOriginDescription: str
  # dataOriginType: Code
  dataOriginType: str
  populationIds: List[str] = []
  instanceType: Literal['StudyArm']

  @classmethod
  def list(cls, uuid, page, size, filter):
    return cls.base_list("MATCH (m:StudyDesign {uuid: '%s'})-[]->(n:StudyArm)" % (uuid), "ORDER BY n.id ASC", page, size, filter)

  @classmethod
  def next_id(cls):
    study_arms = cls.base_list("MATCH (n:StudyArm)", "ORDER BY n.id ASC", page = 0, size = 1, filter = "")
    ids = [item['id'] for item in study_arms['items']]
    nums = [int(id.split('_')[-1]) for id in ids]
    m = max(nums)
    return "StudyArm_"+str(m+1) if m > 0 else "StudyArm_1" 


  @classmethod
  def create(cls, name, description, label):
    try:
      db = Neo4jConnection()
      with db.session() as session:
        next_id = cls.next_id()
        result = session.execute_write(cls._create_study_arm, next_id, name, description, label)
        if not result:
          return {'error': "Failed to create study arm, operation failed"}
        return result 
    except Exception as e:
      logging.error(f"Exception raised while creating study arm")
      logging.error(f"Exception {e}\n{traceback.format_exc()}")
      return {'error': f"Exception. Failed to create study arm"}

  # @classmethod
  # def create(cls, uuid, name, description):
  #   db = Neo4jConnection()
  #   with db.session() as session:
  #     x = session.execute_read(cls._exists, uuid, name)
  #     print("x---", x)
  #     if not session.execute_read(cls._exists, uuid, name):
  #       arms = session.execute_read(cls._epochs, uuid)
  #       return session.execute_write(cls._create_arm, uuid, name, description, arms)
  #     else:
  #       return None

#   @staticmethod
#   def _create_arm(tx, uuid, name, description, epochs):
#       #print("EPOCH", epochs)
#       query = """
#         MATCH (sd:StudyDesign { uuid: $uuid1 })
#         CREATE (a1:StudyArm { studyArmName: $name, studyArmDesc: $desc, uuid: $uuid2 })
#         WITH sd, a1
#         UNWIND $epochs AS epoch
#           MATCH (se:StudyEpoch { uuid: epoch })
#           CREATE (c1:StudyCell { uuid: $uuid3 })
#           CREATE (sd)-[:STUDY_CELL]->(c1)
#           CREATE (c1)-[:STUDY_ARM]->(a1)
#           CREATE (c1)-[:STUDY_EPOCH]->(se)
#         RETURN a1.uuid as uuid
#       """
#       result = tx.run(query, name=name, desc=description, uuid1=uuid, uuid2=str(uuid4()), uuid3=str(uuid4()), epochs=epochs)
# #      try:
#       for row in result:
#         return row["uuid"]
#       return None
# #      except ServiceUnavailable as exception:
# #        logging.error("{query} raised an error: \n {exception}".format(
# #          query=query, exception=exception))
# #        raise

#   @staticmethod
#   def _epochs(tx, uuid):
#     query = """
#       MATCH (sd:StudyDesign { uuid: '%s' })-[:STUDY_CELL]->(c)-[:STUDY_EPOCH]->(a:StudyEpoch)
#       RETURN DISTINCT a.uuid as uuid
#     """ % (uuid)
#     result = tx.run(query)
#     results = []
#     for row in result:
#       results.append(row["uuid"])
#     return results

#   @staticmethod
#   def _exists(tx, uuid, name):
#     query = (
#       "MATCH (sd:StudyDesign { uuid: $uuid })-[:STUDY_CELL]->(c)-[:STUDY_ARM]->(a:StudyArm { studyArmName: $name })"
#       "RETURN a.uuid as uuid"
#     )
#     result = tx.run(query, name=name, uuid=uuid)
#     if result.peek() == None:
#       return False
#     else:
#       return True

  @staticmethod
  def _create_study_arm(tx, next_id, name, description, label, dataOriginDescription = "tbd", dataOriginType = "tbd"):
    uuids = {'StudyArm': str(uuid4())}
    query = """
      CREATE (s:StudyArm {id: $s_id, uuid: $s_uuid1})
      set s.name = $s_name
      set s.description = $s_description
      set s.label = $s_label
      set s.instanceType = 'StudyArm'
      set s.dataOriginDescription = $s_dataOriginDescription
      set s.dataOriginType = $s_dataOriginType
      set s.delete = 'me'
      set s.type = "tbd"
      RETURN s.uuid as uuid
    """
    # print("query",query)
    result = tx.run(query, 
      s_id=next_id,
      s_name=name, 
      s_description=description, 
      s_label=label, 
      s_dataOriginDescription=dataOriginDescription,
      s_dataOriginType=dataOriginType,
      s_uuid1=uuids['StudyArm']
    )
    for row in result:
      return uuids['StudyArm']
    return None

