import traceback
import logging
from typing import Literal, Union
from .base_node import *
from .code import Code
from uuid import uuid4

# class StudyEpochIn(BaseModel):
#   name: str
#   description: str
  
class StudyEpoch(NodeNameLabelDesc):
  # type: Code
  previousId: Union[str, None] = None
  nextId: Union[str, None] = None
  instanceType: Literal['StudyEpoch']

  @classmethod
  def list(cls, uuid, page, size, filter):
    return cls.base_list("MATCH (m:StudyDesign {uuid: '%s'})-[]->(n:StudyEpoch)" % (uuid), "ORDER BY n.id ASC", page, size, filter)

  @classmethod
  def list_with_elements(cls, uuid):
    return cls._list_with_elements(uuid)

  @classmethod
  def list_with_timepoints(cls, uuid):
    return cls._list_with_timepoints(uuid)

  @classmethod
  def next_id(cls):
    study_epochs = cls.base_list("MATCH (n:StudyEpoch)", "ORDER BY n.id ASC", page = 0, size = 1, filter = "")
    ids = [item['id'] for item in study_epochs['items']]
    nums = [int(id.split('_')[-1]) for id in ids]
    m = max(nums)
    return "StudyEpoch_"+str(m+1) if m > 0 else "StudyEpoch_1" 


  @classmethod
  def create(cls, name, description, label):
    try:
      db = Neo4jConnection()
      with db.session() as session:
        next_id = cls.next_id()
        result = session.execute_write(cls._create_study_epoch, next_id, name, description, label)
        if not result:
          return {'error': "Failed to create study epoch, operation failed"}
        return result 
    except Exception as e:
      logging.error(f"Exception raised while creating study epoch")
      logging.error(f"Exception {e}\n{traceback.format_exc()}")
      return {'error': f"Exception. Failed to create study epoch"}

  # @classmethod
  # def create(cls, uuid, name, description):
  #   db = Neo4jConnection()
  #   with db.session() as session:
  #     if not session.execute_read(cls._exists, uuid, name):
  #       arms = session.execute_read(cls._arms, uuid)
  #       return session.execute_write(cls._create_epoch, uuid, name, description, arms)
  #     else:
  #       return None

#   def update(self, name, description):
#     db = Neo4jConnection()
#     with db.session() as session:
#       return session.execute_write(self._update, str(self.uuid), name, description)

#   def add_encounter(self, uuid):
#     db = Neo4jConnection()
#     with db.session() as session:
#       return session.execute_write(self._add_encounter, str(self.uuid), uuid)

  @staticmethod
  def _create_study_epoch(tx, next_id, name, description, label):
    uuids = {'StudyEpoch': str(uuid4())}
    query = """
      CREATE (s:StudyEpoch {id: $s_id, uuid: $s_uuid1})
      set s.name = $s_name
      set s.description = $s_description
      set s.label = $s_label
      set s.instanceType = 'StudyEpoch'
      set s.delete = 'me'
      RETURN s.uuid as uuid
    """
    # print("query",query)
    result = tx.run(query, 
      s_id=next_id,
      s_name=name, 
      s_description=description, 
      s_label=label, 
      s_uuid1=uuids['StudyEpoch']
    )
    for row in result:
      return uuids['StudyEpoch']
    return None

  @staticmethod
  def _list_with_elements(uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign { uuid: $uuid1 })-[:ARMS_REL]->(arm:StudyArm)
        MATCH (arm)<-[:ARM_REL]-(cell:StudyCell)
        MATCH (cell)-[:ELEMENT_REL]->(element:StudyElement)
        MATCH (cell)-[:EPOCH_REL]->(epoch:StudyEpoch)
        return distinct
        arm.name as arm_name
        ,arm.label as arm_label
        ,arm.description as arm_description
        ,epoch.id as epoch_id
        ,epoch.name as epoch_name
        ,epoch.label as epoch_label
        ,epoch.description as epoch_labeldescription
        ,element.name as element_name
        ,element.label as element_label
        ,element.description as element_description
        ,arm.uuid as arm_uuid
        ,epoch.uuid as epoch_uuid
        ,element.uuid as element_uuid
        order by arm_name, epoch_id, element_name
      """
      # print("query",query)
      # print("uuid", uuid)
      response = session.run(query, uuid1=uuid)
      results = []
      for row in response.data():
        results.append(row)
    db.close()
    return { "items": results}

  @staticmethod
  def _list_with_timepoints(uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign { uuid: $uuid1 })-[:ARMS_REL]->(arm:StudyArm)
        match (arm)<-[:ARM_REL]-(cell:StudyCell)-[:EPOCH_REL]->(epoch:StudyEpoch)<-[:EPOCH_REL]-(sai:ScheduledActivityInstance)
        match (sai)-[:ENCOUNTER_REL]-(e:Encounter)
        match (e)-[:SCHEDULED_AT_REL]->(timing:Timing)
        return distinct
        arm.id as arm_id
        ,arm.name as arm_name
        ,epoch.id as epoch_id
        ,epoch.name as epoch_name
        ,e.name as encounter_name
        ,timing.value as timing_value
        ,timing.name as timing_name
        order by arm_id, epoch_id
      """
      # print("query",query, uuid)
      # print("uuid", uuid)
      response = session.run(query, uuid1=uuid)
      results = []
      for row in response.data():
        results.append(row)
    db.close()
    return { "items": results}


#   @staticmethod
#   def _create_epoch(tx, uuid, name, description, arms):
#       #print("ARMS", arms)
#       query = """
#         MATCH (sd:StudyDesign { uuid: $uuid1 })-[:STUDY_CELL]->(c:StudyCell)-[:STUDY_EPOCH]->(a:StudyEpoch)
#         WHERE NOT (a)-[:NEXT_EPOCH]->()
#         CREATE (a1:StudyEpoch { studyEpochName: $name, studyEpochDesc: $desc, uuid: $uuid2 })
#         CREATE (a)-[:NEXT_EPOCH]->(a1)
#         CREATE (a1)-[:PREVIOUS_EPOCH]->(a)
#         WITH sd, a1
#         UNWIND $arms AS arm
#           MATCH (sa:StudyArm { uuid: arm })
#           CREATE (c1:StudyCell { uuid: $uuid3 })
#           CREATE (sd)-[:STUDY_CELL]->(c1)
#           CREATE (c1)-[:STUDY_ARM]->(sa)
#           CREATE (c1)-[:STUDY_EPOCH]->(a1)
#         RETURN a1.uuid as uuid
#       """
#       result = tx.run(query, name=name, desc=description, uuid1=uuid, uuid2=str(uuid4()), uuid3=str(uuid4()), arms=arms)
# #      try:
#       for row in result:
#         return row["uuid"]
#       return None
# #      except ServiceUnavailable as exception:
# #        logging.error("{query} raised an error: \n {exception}".format(
# #          query=query, exception=exception))
# #        raise

#   @staticmethod
#   def _update(tx, uuid, name, description):
#     query = """
#       MATCH (ep:StudyEpoch { uuid: $uuid1 })
#       SET ep.studyEpochName = $name, ep.studyEpochDesc = $desc
#       RETURN ep
#     """
#     result = tx.run(query, uuid1=uuid, name=name, desc=description)
#     for row in result:
#       return StudyEpoch(**row['ep'])
#     return None

#   @staticmethod
#   def _arms(tx, uuid):
#     query = """
#       MATCH (sd:StudyDesign { uuid: '%s' })-[:STUDY_CELL]->(c)-[:STUDY_ARM]->(a:StudyArm)
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
#       "MATCH (sd:StudyDesign { uuid: $uuid })-[:STUDY_CELL]->(c)-[:STUDY_EPOCH]->(a:StudyEpoch { studyEpochName: $name })"
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
#       "MATCH (sd:StudyDesign { uuid: $uuid })-[:STUDY_CELL]->(c)-[:STUDY_EPOCH]->(a:StudyEpoch)"
#       "RETURN a.uuid"
#     )
#     result = tx.run(query, uuid=uuid)
#     if result.peek() == None:
#       return False
#     else:
#       return True

#   @staticmethod
#   def _add_encounter(tx, epoch_uuid, encounter_uuid):
#     query = (
#       "MATCH (ep:StudyEpoch { uuid: $uuid1 }), (e:Encounter { uuid: $uuid2 })"
#       "MERGE (ep)-[:ENCOUNTER]->(e)"
#     )
#     result = tx.run(query, uuid1=epoch_uuid, uuid2=encounter_uuid)
#     return encounter_uuid

