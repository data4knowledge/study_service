from model.neo4j_connection import Neo4jConnection

class StudyDesignSubjectData():

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
      query = """MATCH (sd:StudyDesign {uuid: '%s'})<-[]-(subj:Subject)<-[]-(dp:DataPoint)-[]->(dtp:StudyBCDataTypeProperty)
        <-[]-(dt:StudyBCDataType)<-[]-(i:StudyBCItem)<-[]-(bc:StudyBCInstance), (subj)-[]->(si:Site) 
        RETURN COUNT(dp) AS count
      """ % (uuid)
      result = session.run(query)
      count = 0
      for record in result:
        count = record['count']
      query = """MATCH (sd:StudyDesign {uuid: '%s'})<-[]-(subj:Subject)<-[]-(dp:DataPoint)-[]->(dtp:StudyBCDataTypeProperty)
        <-[]-(dt:StudyBCDataType)<-[]-(i:StudyBCItem)<-[]-(bc:StudyBCInstance), (subj)-[]->(si:Site) 
        RETURN subj.identifier as subject, dp.value as value, si.identifier as site, dtp.uri as data_uri, dtp.name as property, dt.name as data_type, i.name as item, bc.name as bc 
        ORDER BY site, subject, bc, item, data_type, property %s
      """ % (uuid, skip_offset_clause)
      print(query)
      result = session.run(query)
      results = []
      for record in result:
        final_record = { 
          "subject": record["subject"], 
          "site": record["site"], 
          "value": record["value"], 
          "bc_name": record["bc"], 
          "item": record["item"], 
          "data_type": record["data_type"], 
          "property": record["property"], 
          "data_uri": record["data_uri"] 
        }
        results.append(final_record)
    result = {'items': results, 'page': page, 'size': size, 'filter': filter, 'count': count }
    return result
