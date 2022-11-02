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
        MATCH path=(sd:StudyDesign {uuid: '%s'})-[]->(v:Encounter)-[r:NEXT_ENCOUNTER *0..]->()
        WITH v ORDER BY LENGTH(path) DESC
        MATCH (e:StudyEpoch)-[]->(v)
        WITH e.studyEpochName as epoch,v.encounterName as visit 
        RETURN DISTINCT epoch, visit
      """ % (uuid)
      result = session.run(query)
      for record in result:
        #print(record)
        if not record["epoch"] in epoch_visits:
          epoch_visits[record["epoch"]] = []    
          epoch_count += 1
        epoch_visits[record["epoch"]].append(record["visit"])
        visits[record["visit"]] = record["epoch"]
        visit_row[record["visit"]] = ""
      #print(visits)

      # Visit Rules
      query = """MATCH (sd:StudyDesign {uuid: '%s'})-[]->(sc:StudyCell)-[]->(e:StudyEpoch)
          -[]->(v:Encounter)
          WITH v 
          RETURN v.encounterName as visit""" % (uuid)
      result = session.run(query)
      for visit in visits.keys():
          visit_rule[visit] = ""
      for record in result:
          #if record["start_rule"] == record["end_rule"]:
          #    visit_rule[record["visit"]] = "%s" % (record["start_rule"])
          #else:
        visit_rule[record["visit"]] =  "not set" #"%s to %s" % (record["start_rule"], record["end_rule"])

      # Activities
      query = """MATCH (sd:StudyDesign {uuid: '%s'})-[]->(sc:StudyCell)-[]->(e:StudyEpoch)
          -[]->(v:Encounter)<-[]-(wfi:WorkflowItem)-[]->(a:Activity) 
          WITH a.activityName as activity, v.encounterName as visit
          RETURN DISTINCT activity, visit""" % (uuid)
      result = session.run(query)
      activities = {}
      for record in result:
        if not record["activity"] in activities:
          activities[record["activity"]] = visit_row.copy()
        activities[record["activity"]][record["visit"]] = "X" 
      
      # Activity Order
      activity_order = []
      query = """
        MATCH path=(sd:StudyDesign {uuid: '%s'})-[]->(a:Activity)-[r:NEXT_ACTIVITY *0..]->()
        WITH a ORDER BY LENGTH(path) DESC
        RETURN DISTINCT a.activityName as name, a.uuid as uuid
      """  % (uuid)
      result = session.run(query)
      for record in result:
        activity_order.append({ 'name': record["name"], 'uuid': record['uuid'] })
      #print(activity_order)

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