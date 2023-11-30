from typing import List, Union
from model.node import *
from .schedule_timeline_exit import ScheduleTimelineExit
from .scheduled_instance import ScheduledActivityInstance, ScheduledDecisionInstance

class ScheduleTimeline(NodeNameLabelDesc):
  mainTimeline: bool
  entryCondition: str
  entryId: str
  exits: List[ScheduleTimelineExit] = []
  instances: List[Union[ScheduledActivityInstance, ScheduledDecisionInstance]] = []

  @classmethod
  def list(cls, uuid, page, size, filter):
    return cls.base_list("MATCH (m:StudyDesign {uuid: '%s'})-[]->(n:ScheduleTimeline)" % (uuid), "ORDER BY n.name ASC", page, size, filter)
