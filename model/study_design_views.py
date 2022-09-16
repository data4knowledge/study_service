from model.neo4j_connection import Neo4jConnection

class StudyDesignViews():

  def soa(self, uuid):
    db = Neo4jConnection()
    with db.session() as session:
      visits = {}
      visit_row = {}
      visit_rule = {}
      epoch_visits = {}
      epoch_count = 0

      # Epochs and Visits
      query = """MATCH (sd:StudyDesign {uuid: '%s'})-[]->(sc:StudyCell)-[]->
          (e:StudyEpoch)-[]->(v:Encounter)
          WITH e.studyEpochName as epoch,v.encounterName as visit 
          RETURN DISTINCT epoch, visit""" % (uuid)
      result = session.run(query)
      for record in result:
          if not record["epoch"] in epoch_visits:
              epoch_visits[record["epoch"]] = []    
              epoch_count += 1
          epoch_visits[record["epoch"]].append(record["visit"])
          visits[record["visit"]] = record["epoch"]
          visit_row[record["visit"]] = ""

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
      query = """MATCH path=(sd:StudyDesign {uuid: '%s'})-[]->(a:Activity)-[r:NEXT_ACTIVITY *1..]->(b:Activity) RETURN a.activityName as desc""" % (uuid)
      result = session.run(query)
      for record in result:
        activity_order.append(record["desc"])
      query = """MATCH path=(sd:StudyDesign {uuid: '%s'})-[]->(a:Activity)-[r:NEXT_ACTIVITY *1..]->(b:Activity) RETURN b.activityName as desc ORDER BY LENGTH(path) ASC;"""  % (uuid)
      result = session.run(query)
      for record in result:
        activity_order.append(record["desc"])

      # Return the results
      results = []
      results.append([""] + list(visits.values()))
      results.append([""] + list(visits.keys()))
      results.append([""] + list(visit_rule.values()))
      for activity in activity_order:
        if activity in activities:
          data = activities[activity]
          results.append([activity] + list(data.values()))
    return results

  def data_contract(self, uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = """MATCH (sd:StudyDesign {uuid: '%s'})-[]->(sc:StudyCell)-[]->(e:StudyEpoch)
          -[]->(v:Encounter)<-[]-(wfi:WorkflowItem)-[]->(a:Activity)-[]->(sda:StudyData)
          WITH a.activityName as activity, v.encounterName as visit, sda.studyDataDesc as study_data, sda.ecrfLink as ecrf_link
          RETURN DISTINCT activity, visit, study_data, ecrf_link""" % (uuid)
      result = session.run(query)
      results = []
      for record in result:
        results.append({ "visit": record["visit"], "activity": record["activity"], "study_data": record["study_data"], "ecrf_link": record["ecrf_link"] })
    return results