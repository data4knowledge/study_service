from model.neo4j_connection import Neo4jConnection

class StudyDesignDataContract():

  @classmethod
  def create(cls, uuid):
    pass

  @classmethod
  def read(cls, uuid, page, size, filter):
    skip_offset_clause = ""
    if page != 0:
      offset = (page - 1) * size
      skip_offset_clause = "SKIP %s LIMIT %s" % (offset, size)
    db = Neo4jConnection()
    with db.session() as session:
      query = """MATCH (sd:StudyDesign {uuid: '%s'})-[]->(wf:Workflow)-[]->(wfi:WorkflowItem)-[]->(bc:StudyBCInstance)
        RETURN COUNT(bc) as count
      """ % (uuid)
      result = session.run(query)
      count = 0
      for record in result:
        count = record['count']
      query = """MATCH (sd:StudyDesign {uuid: '%s'})-[]->(wf:Workflow)-[]->(wfi:WorkflowItem)
        WITH wfi
        MATCH (a:Activity)<-[]-(wfi)-[]->(v:Encounter), (wfi)-[]->(bc:StudyBCInstance)
        WITH a.activityName as activity, v.encounterName as visit, bc.name as bc_name, bc.uri as data_uri, bc.reference_uri as bc_uri
        RETURN DISTINCT activity, visit, bc_name, bc_uri, data_uri ORDER BY visit, activity %s""" % (uuid, skip_offset_clause)
      result = session.run(query)
      results = []
      for record in result:
        results.append({ "visit": record["visit"], "activity": record["activity"], "bc_name": record["bc_name"], "bc_uri": record["bc_uri"], "data_uri": record["data_uri"] })
    result = {'items': results, 'page': page, 'size': size, 'filter': filter, 'count': count }
    return result
