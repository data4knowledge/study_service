from model.neo4j_connection import Neo4jConnection

class StudyDesignDataContract():

  @classmethod
  def create(cls, uuid):
    pass

  @classmethod
  def read(cls, uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = """MATCH (sd:StudyDesign {uuid: '%s'})-[]->(sc:StudyCell)-[]->(e:StudyEpoch)
          -[]->(v:Encounter)<-[]-(wfi:WorkflowItem)-[]->(a:Activity)-[]->(sda:StudyData)
          WITH a.activityName as activity, v.encounterName as visit, sda.studyDataName as study_data, sda.crfLink as crf_link
          RETURN DISTINCT activity, visit, study_data, crf_link ORDER BY visit, activity, study_data""" % (uuid)
      result = session.run(query)
      results = []
      for record in result:
        #print(record)
        results.append({ "visit": record["visit"], "activity": record["activity"], "study_data": record["study_data"], "crf_link": record["crf_link"] })
    return results