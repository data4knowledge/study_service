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
      # -[:HAS_STUDY_BC_DATA_TYPE_PROPERTY *1..]->(dtp:StudyBCDataTypeProperty)
      query = """MATCH (sd:StudyDesign {uuid: '%s'})-[]->(wf:Workflow)-[]->(wfi:WorkflowItem)-[]->(bc:StudyBCInstance)-[]->(i:StudyBCItem)-[]->(dt:StudyBCDataType) WHERE i.collect="True"
        RETURN COUNT(dt) as count
      """ % (uuid)
      result = session.run(query)
      count = 0
      for record in result:
        count = record['count']
      # -[:HAS_STUDY_BC_DATA_TYPE_PROPERTY *1..]->(dtp:StudyBCDataTypeProperty)
      query = """MATCH (sd:StudyDesign {uuid: '%s'})-[]->(wf:Workflow)-[]->(wfi:WorkflowItem)
        WITH wfi
        MATCH (a:Activity)<-[]-(wfi)-[]->(v:Encounter), (wfi)-[]->(bc:StudyBCInstance)-[]->(i:StudyBCItem)-[]->(dt:StudyBCDataType) WHERE i.collect="True"
        WITH a.activityName as activity, v.encounterName as visit, bc.name as bc_name, dt.uri as data_uri, bc.reference_uri as bc_uri, i.name as item, dt.name as data_type
        RETURN DISTINCT activity, visit, bc_name, bc_uri, data_uri, item, data_type ORDER BY visit, activity, bc_name, item %s""" % (uuid, skip_offset_clause)
      result = session.run(query)
      results = []
      for record in result:
        results.append({ "visit": record["visit"], "activity": record["activity"], "bc_name": record["bc_name"], "bc_uri": record["bc_uri"], "item": record["item"], "data_type": record["data_type"], "data_uri": record["data_uri"] })
    result = {'items': results, 'page': page, 'size': size, 'filter': filter, 'count': count }
    return result
