from typing import List, Literal
from .base_node import *
from .code import Code

# class StudyArmIn(BaseModel):
#   name: str
#   description: str

class StudyArm(NodeNameLabelDesc):
  type: Code
  dataOriginDescription: str
  dataOriginType: Code
  populationIds: List[str] = []
  instanceType: Literal['StudyArm']

  @classmethod
  def list(cls, uuid, page, size, filter):
    return cls.base_list("MATCH (m:StudyDesign {uuid: '%s'})-[]->(n:StudyArm)" % (uuid), "ORDER BY n.id ASC", page, size, filter)

  @classmethod
  def create(cls, uuid, name, description):
    db = Neo4jConnection()
    with db.session() as session:
      if not session.execute_read(cls._exists, uuid, name):
        arms = session.execute_read(cls._epochs, uuid)
        return session.execute_write(cls._create_arm, uuid, name, description, arms)
      else:
        return None

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


