import traceback
import logging
from typing import List, Literal, Dict, Union
from .base_node import *
from .timing import Timing
from uuid import uuid4

class ScheduledInstance(NodeId):
  timelineId: Union[str, None] = None
  timelineExitId: Union[str, None] = None
  defaultConditionId: Union[str, None] = None
  epochId: Union[str, None] = None
  instanceType: Literal['ScheduledInstance']

class ScheduledActivityInstance(ScheduledInstance):
  activityIds: List[str] = []
  encounterId: Union[str, None] = None
  instanceType: Literal['ScheduledActivityInstance']

  @classmethod
  def next_id(cls):
    scheduled_activity_instances = cls.base_list("MATCH (n:ScheduledActivityInstance)", "ORDER BY n.id ASC", page = 0, size = 1, filter = "")
    ids = [item['id'] for item in scheduled_activity_instances['items']]
    nums = [int(id.split('_')[-1]) for id in ids]
    m = max(nums)
    return "ScheduledActivityInstance"+str(m+1) if m > 0 else "ScheduledActivityInstance_1"


  @classmethod
  def create(cls, name, description, label):
    try:
      db = Neo4jConnection()
      with db.session() as session:
        next_id = cls.next_id()
        result = session.execute_write(cls._create_study_activity_instance, next_id, name, description, label)
        if not result:
          return {'error': "Failed to create scheduled activity instance, operation failed"}
        return result 
    except Exception as e:
      logging.error(f"Exception raised while creating scheduled activity instance")
      logging.error(f"Exception {e}\n{traceback.format_exc()}")
      return {'error': f"Exception. Failed to create study arm"}

  # timelineId: Union[str, None] = None
  # timelineExitId: Union[str, None] = None
  # defaultConditionId: Union[str, None] = None
  # epochId: Union[str, None] = None
  # activityIds: List[str] = []
  # encounterId: Union[str, None] = None
  # instanceType: Literal['ScheduledActivityInstance']
  @staticmethod
  def _create_study_activity_instance(tx, next_id, name, description, label):
    uuids = {'ScheduledActivityInstance': str(uuid4())}
    query = """
      CREATE (s:ScheduledActivityInstance {id: $s_id, uuid: $s_uuid1})
      set s.name = $s_name
      set s.description = $s_description
      set s.label = $s_label
      set s.instanceType = 'ScheduledActivityInstance'
      set s.delete = 'me'
      set s.type = "tbd"
      RETURN s.uuid as uuid
    """
    print("query",query)
    result = tx.run(query, 
      s_id=next_id,
      s_name=name, 
      s_description=description, 
      s_label=label, 
      s_uuid1=uuids['ScheduledActivityInstance']
    )
    for row in result:
      return uuids['ScheduledActivityInstance']
    return None


class ScheduledDecisionInstance(ScheduledInstance):
  conditionAssignments: Dict[str, str]
  instanceType: Literal['ScheduledDecisionInstance']
