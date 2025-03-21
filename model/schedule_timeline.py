import traceback
import logging
from typing import List, Literal, Union
from .base_node import *
from .schedule_timeline_exit import ScheduleTimelineExit
from .scheduled_instance import ScheduledActivityInstance, ScheduledDecisionInstance
from .timing import Timing
from uuid import uuid4

class ScheduleTimeline(NodeNameLabelDesc):
  mainTimeline: bool
  entryCondition: str
  entry: ScheduledActivityInstance = None
  exits: List[ScheduleTimelineExit] = []
  timings: List[Timing] = []
  instances: List[Union[ScheduledActivityInstance, ScheduledDecisionInstance]] = []
  instanceType: Literal['ScheduleTimeline']

  @classmethod
  def create(cls, name, description, label, template_uuid):
    try:
      db = Neo4jConnection()
      with db.session() as session:
        result = session.execute_write(cls._create_scheduled_timeline, name, description, label, template_uuid)
        if not result:
          return {'error': "Failed to create timeline, operation failed"}
        return result 
    except Exception as e:
      logging.error(f"Exception raised while creating timeline")
      logging.error(f"Exception {e}\n{traceback.format_exc()}")
      return {'error': f"Exception. Failed to create timeline"}

  @classmethod
  def list(cls, uuid, page, size, filter):
    return cls.base_list("MATCH (m:StudyDesign {uuid: '%s'})-[]->(n:ScheduleTimeline)" % (uuid), "ORDER BY n.name ASC", page, size, filter)

  def soa(self):
    db = Neo4jConnection()
    with db.session() as session:

      # Activity order
      query = """MATCH (st:ScheduleTimeline {uuid: '%s'})<-[]-(sd:StudyDesign)-[]->(a1:Activity)
        WHERE NOT (a1)-[:PREVIOUS_REL]->()
        WITH a1 
        MATCH path=(a1)-[:NEXT_REL *0..]->(a)
        WITH a ORDER BY LENGTH(path) ASC
        RETURN DISTINCT a.name as name, a.label as label, a.uuid as uuid
      """ % (self.uuid)
      #print(f"ACTIVITY Q: {query}")
      result = session.run(query)
      activity_order = []
      for record in result:
        activity_order.append({'name': record['name'], 'label': record['label'], 'uuid': record['uuid']})
      #print(f"ACTIVITY ORDER: {activity_order}")
      #print("")
      #print("")

      # Epochs and Visits
      query = """
        MATCH (st:ScheduleTimeline {uuid: '%s'})-[:ENTRY_REL]->(sai1:ScheduledActivityInstance)
        WITH sai1
        MATCH path=(sai1)-[:DEFAULT_CONDITION_REL *0..]->(sai)
        WITH sai ORDER BY LENGTH(path) ASC
        OPTIONAL MATCH (e:StudyEpoch)<-[]-(sai)
        OPTIONAL MATCH (sai)-[]->(v:Encounter)
        OPTIONAL MATCH (sai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
        RETURN DISTINCT e.name as epoch_name, e.label as epoch_label, v.name as visit_name, v.label as visit_label, t.windowLabel as window, t.label as tvalue, sai.uuid as uuid 
      """ % (self.uuid)
      #print(f"ACTIVITY INSTANCES QUERY: {query}")
      result = session.run(query)
      ai = []
      for index, record in enumerate(result):
        window = record['window'] if record['window'] else ''
        entry = {
          'instance': {'uuid': record['uuid'], 'name': f'SAI{index+1}', 'label': f'Instance {index+1}', 'label1': record['tvalue']},
          'epoch': {'name': record['epoch_name'], 'label': record['epoch_label']},
          'visit': {'name': record['visit_name'], 'label': record['visit_label'], 'window': window}
        }
        ai.append(entry)
      #print(f"ACTIVITY INSTANCES: {ai}")
      #print("")
      #print("")
      
      visit_row = {}
      for item in ai:
        visit_row[item['instance']['uuid']] = ''
      #print(f"VISIT ROW: {visit_row}")
      #print("")
      #print("")
      
      # Activities
      query = """
        UNWIND $instances AS uuid
          MATCH (sai:ScheduledActivityInstance {uuid: uuid})-[]->(a:Activity) 
        RETURN sai.uuid as uuid, a.name as name, a.label as label
      """
      #print(f"ACTIVITIES QUERY: {query}")
      instances = [item['instance']['uuid'] for item in ai]
      #print(f"INSTANCES: {instances}")
      result = session.run(query, instances=instances)
      activities = {}
      for record in result:
        if not record["name"] in activities:
          activities[record["name"]] = visit_row.copy()
        activities[record["name"]][record["uuid"]] = "X" 
      #print(f"ACTIVITIES: {activities}")
      #print("")
      #print("")
      
      # Return the results
      labels = []
      for item in ai:
        label = item['visit']['label'] if item['visit']['label'] else item['instance']['label']
        labels.append(label)
      results = []
      results.append([""] + [item['epoch']['label'] for item in ai])
      results.append([""] + labels)
      results.append([""] + [item['instance']['label1'] for item in ai])
      results.append([""] + [item['visit']['window'] for item in ai])
      for activity in activity_order:
        if activity['name'] in activities:
          data = activities[activity['name']]
          label = activity['label'] if activity['label'] else activity['name'] 
          results.append([{'name': label, 'uuid': activity['uuid']}] + list(data.values()))
    return results

  @staticmethod
  def _create_scheduled_timeline(tx, name, description, label, template_uuid):
    uuids = {'ScheduleTimeline': str(uuid4())}
    query = """
      CREATE (s:ScheduleTimeline {id: $s_id, uuid: $s_uuid1, name: $s_name, description: $s_description, label: $s_label, mainTimeline: 'true', instanceType: 'ScheduleTimeline'})
      set s.delete = 'me'
      RETURN s.uuid as uuid
    """
    # print("query",query)
    result = tx.run(query, 
      s_id='ADDED_SCHEDULE_TIMELINE',
      s_name=name, 
      s_description=description, 
      s_label=label, 
      s_uuid1=uuids['ScheduleTimeline']
    )
    for row in result:
      return uuids['ScheduleTimeline']
    return None
