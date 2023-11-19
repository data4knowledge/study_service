from model.neo4j_connection import Neo4jConnection

class StudyDesignSOA():

  @classmethod
  def read(cls, uuid):
    db = Neo4jConnection()
    with db.session() as session:
      visits = {}
      visit_row = {}
      visit_rule = {}
      epoch_visits = {}
      epoch_count = 0

      # Epochs and Visits
      query = """
        MATCH path=(sd:StudyDesign {uuid: '%s'})-[]->(v:Encounter)-[r:NEXT *0..]->()
        WITH v ORDER BY LENGTH(path) DESC
        MATCH (e:StudyEpoch)<-[]-(sai:ScheduledActivityInstance)-[]->(v)
        WITH e.name as epoch,v.name as visit 
        RETURN DISTINCT epoch, visit
      """ % (uuid)
      print(f"SOA1: {query}")
      result = session.run(query)
      for record in result:
        if not record["epoch"] in epoch_visits:
          epoch_visits[record["epoch"]] = []    
          epoch_count += 1
        epoch_visits[record["epoch"]].append(record["visit"])
        visits[record["visit"]] = record["epoch"]
        visit_row[record["visit"]] = ""
      print(f"SOA2: {epoch_visits} {visits} {visit_row}")

      # Visit Rules
      query = """MATCH (sd:StudyDesign {uuid: '%s'})-[]->(sc:StudyCell)-[]->(e:StudyEpoch)<-[]-(sai:ScheduledActivityInstance)-[]->(v:Encounter)
          WITH v 
          RETURN v.name as visit""" % (uuid)
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
      query = """MATCH (sd:StudyDesign {uuid: '%s'})-[]->(sc:StudyCell)-[]->(e:StudyEpoch)<-[]-(sai:ScheduledActivityInstance)
          WITH sai
          MATCH (v:Encounter)<-[]-(sai)-[]->(a:Activity) 
          WITH a.name as activity, v.name as visit
          RETURN DISTINCT activity, visit""" % (uuid)
      print(f"SOA5: {query}")
      result = session.run(query)
      activities = {}
      for record in result:
        if not record["activity"] in activities:
          activities[record["activity"]] = visit_row.copy()
        activities[record["activity"]][record["visit"]] = "X" 
      print(f"SOA6: {activities}")
      
      # Activity Order
      activity_order = []
      query = """
        MATCH path=(sd:StudyDesign {uuid: '%s'})-[]->(a:Activity)-[r:NEXT *0..]->()
        WITH a ORDER BY LENGTH(path) DESC
        RETURN DISTINCT a.name as name, a.uuid as uuid
      """  % (uuid)
      print(f"SOA7: {query}")
      result = session.run(query)
      for record in result:
        activity_order.append({ 'name': record["name"], 'uuid': record['uuid'] })
      print(f"SOA8: {activity_order}")

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