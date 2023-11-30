from typing import List, Union
from .node import *
from .neo4j_connection import Neo4jConnection
from .schedule_timeline_exit import ScheduleTimelineExit
from .scheduled_instance import ScheduledActivityInstance, ScheduledDecisionInstance

class ScheduleTimeline(NodeNameLabelDesc):
  mainTimeline: bool
  entryCondition: str
  entry: ScheduledActivityInstance = None
  exits: List[ScheduleTimelineExit] = []
  instances: List[Union[ScheduledActivityInstance, ScheduledDecisionInstance]] = []

  @classmethod
  def list(cls, uuid, page, size, filter):
    return cls.base_list("MATCH (m:StudyDesign {uuid: '%s'})-[]->(n:ScheduleTimeline)" % (uuid), "ORDER BY n.name ASC", page, size, filter)

  def soa(self):
    db = Neo4jConnection()
    with db.session() as session:
      visits = {}
      visit_row = {}
      visit_rule = {}
      epoch_visits = {}

      # Encounter order
      query = """MATCH (st:ScheduleTimeline {uuid: '%s'})<-[]-(sd:StudyDesign)-[]->(v:Encounter)
        WHERE NOT (v)-[:PREVIOUS_REL]->()
        WITH v
        MATCH path=(v)-[:NEXT_REL *0..]->(x)
        RETURN DISTINCT x.name as name, x.label as label, x.uuid as uuid
      """ % (self.uuid)
      print(f"EPOCH Q: {query}")
      result = session.run(query)
      visits_order = []
      for record in result:
        visits_order.append({'name': record['name'], 'label': record['label'], 'uuid': record['uuid']})
      print(f"VISIT ORDER: {visits_order}")

      # Activity order
      query = """MATCH (st:ScheduleTimeline {uuid: '%s'})<-[]-(sd:StudyDesign)-[]->(v:Activity)
        WHERE NOT (v)-[:PREVIOUS_REL]->()
        WITH v
        MATCH path=(v)-[:NEXT_REL *0..]->(x)
        RETURN DISTINCT x.name as name, x.label as label, x.uuid as uuid
      """ % (self.uuid)
      print(f"EPOCH Q: {query}")
      result = session.run(query)
      activity_order = []
      for record in result:
        activity_order.append({'name': record['name'], 'label': record['label'], 'uuid': record['uuid']})
      print(f"ACTIVITY ORDER: {activity_order}")

      # Epochs and Visits
      query = """
        MATCH (st:ScheduleTimeline {uuid: '%s'})-[]->(sai:ScheduledActivityInstance) 
        WITH sai
        MATCH (e:StudyEpoch)<-[]-(sai)-[]->(v:Encounter)
        WITH e.name as epoch,v.name as visit 
        RETURN DISTINCT epoch, visit
      """ % (self.uuid)
      print(f"SOA1: {query}")
      result = session.run(query)
      for record in result:
        if not record["epoch"] in epoch_visits:
          epoch_visits[record["epoch"]] = []    
        epoch_visits[record["epoch"]].append(record["visit"])
        visits[record["visit"]] = record["epoch"]
        visit_row[record["visit"]] = ""
      print(f"EV: {epoch_visits}\nV: {visits}\nVR: {visit_row}")

      # Visit Rules
      query = """MATCH (sd:StudyDesign {uuid: '%s'})-[]->(sc:StudyCell)-[]->(e:StudyEpoch)<-[]-(sai:ScheduledActivityInstance)-[]->(v:Encounter)
          WITH v 
          RETURN v.name as visit""" % (self.uuid)
      print(f"SOA3: {query}")
      result = session.run(query)
      for visit in visits.keys():
          visit_rule[visit] = ""
      for record in result:
          #if record["start_rule"] == record["end_rule"]:
          #    visit_rule[record["visit"]] = "%s" % (record["start_rule"])
          #else:
        visit_rule[record["visit"]] =  "not set" #"%s to %s" % (record["start_rule"], record["end_rule"])
      print(f"SOA4: {visit_rule}")

      # Activities
      query = """MATCH (st:ScheduleTimeline {uuid: '%s'})-[]->(sai:ScheduledActivityInstance) 
        WITH sai
        MATCH (v:Encounter)<-[]-(sai)-[]->(a:Activity) 
        WITH a.name as activity, v.name as visit
        RETURN DISTINCT activity, visit""" % (self.uuid)
      print(f"SOA5: {query}")
      result = session.run(query)
      activities = {}
      for record in result:
        if not record["activity"] in activities:
          activities[record["activity"]] = visit_row.copy()
        activities[record["activity"]][record["visit"]] = "X" 
      print(f"SOA6: {activities}")
      
      # # Activity Order
      # activity_order = []
      # query = """
      #   MATCH path=(st:ScheduleTimeline {uuid: '%s'})<-[]-(sd:StudyDesign)-[]->(a:Activity)-[r:NEXT *0..]->()
      #   WITH a ORDER BY LENGTH(path) DESC
      #   RETURN DISTINCT a.name as name, a.uuid as uuid
      # """  % (self.uuid)
      # print(f"SOA7: {query}")
      # result = session.run(query)
      # for record in result:
      #   activity_order.append({ 'name': record["name"], 'uuid': record['uuid'] })
      # print(f"SOA8: {activity_order}")

      # Return the results
      results = []
      results.append([""] + list(visits.values()))
      results.append([""] + list(visits.keys()))
      results.append([""] + list(visit_rule.values()))
      for activity in activity_order:
        if activity['name'] in activities:
          data = activities[activity['name']]
          results.append([activity] + list(data.values()))
    return results