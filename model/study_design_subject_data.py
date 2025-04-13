from d4kms_service import Neo4jConnection

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
      query = """MATCH (sd:StudyDesign {uuid: '%s'})-[:ORGANIZATIONS_REL]->(resOrg:ResearchOrganization)-[:MANAGES_REL]->(site:StudySite)<-[:ENROLLED_AT_SITE_REL]-(subj:Subject)<-[:FOR_SUBJECT_REL]-(dp:DataPoint)
      RETURN COUNT(dp) AS count
      """ % (uuid)
      print("query subject count", query)
      result = session.run(query)
      count = 0
      for record in result:
        count = record['count']
      # query = """MATCH (sd:StudyDesign {uuid: '%s'})-[:ORGANIZATIONS_REL]->(resOrg:ResearchOrganization)-[:MANAGES_REL]->(site:StudySite)<-[:ENROLLED_AT_SITE_REL]-(subj:Subject)<-[:FOR_SUBJECT_REL]-(dp:DataPoint)-[:FOR_DC_REL]->(dc:DataContract)-[:PROPERTIES_REL]->(bc_prop:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept),(s)-[:ENROLLED_AT_SITE_REL]->(site:StudySite)
      query = """MATCH (sd:StudyDesign {uuid: '%s'})-[:ORGANIZATIONS_REL]->(resOrg:ResearchOrganization)-[:MANAGES_REL]->(site:StudySite)<-[:ENROLLED_AT_SITE_REL]-(subj:Subject)<-[:FOR_SUBJECT_REL]-(dp:DataPoint)-[:FOR_DC_REL]->(dc:DataContract)-[:PROPERTIES_REL]->(bc_prop:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
      OPTIONAL MATCH (bc_prop)-[:CODE_REL]->(:AliasCode)-[:STANDARD_CODE_REL]->(prop:Code)
      RETURN distinct subj.identifier as subject, 
      dp.value as value, 
      site.name as site, 
      dp.uri as data_uri, 
      coalesce(prop.decode, bc_prop.generic_name) as property, 
      bc_prop.datatype as data_type, 
      bc_prop.name as item, 
      bc.name as bc,
      dc.uri as contract_uri
      ORDER BY site, subject, bc, item, data_type, property %s
      """ % (uuid, skip_offset_clause)
      # print("subject data", query)
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
          "data_uri": record["data_uri"],
          "contract_uri": record["contract_uri"]
        }
        results.append(final_record)
    result = {'items': results, 'page': page, 'size': size, 'filter': filter, 'count': count }
    return result

  @classmethod
  def subjects(cls, uuid, page, size, filter):
    skip_offset_clause = ""
    if page != 0:
      offset = (page - 1) * size
      skip_offset_clause = "SKIP %s LIMIT %s" % (offset, size)
    db = Neo4jConnection()
    with db.session() as session:
      # NOTE: Only matching example site subjects
      query = """MATCH (sd:StudyDesign {uuid: '%s'})-[:ORGANIZATIONS_REL]->(resOrg:ResearchOrganization)-[:MANAGES_REL]->(site:StudySite {name: '101'})<-[:ENROLLED_AT_SITE_REL]-(subj:Subject)
      RETURN COUNT(subj) AS count
      """ % (uuid)
      result = session.run(query)
      count = 0
      for record in result:
        count = record['count']
      # NOTE: Only matching example site subjects
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[:ORGANIZATIONS_REL]->(resOrg:ResearchOrganization)-[:MANAGES_REL]->(site:StudySite {name: '101'})<-[:ENROLLED_AT_SITE_REL]-(subj:Subject)
        with
          site.name as site,
          subj.identifier as subject,
          toInteger(subj.identifier) as n_subject
        return
          site,
          subject,
          n_subject,
          case n_subject when null then 0 else 1 end as num
        ORDER BY site, num, n_subject %s
      """ % (uuid, skip_offset_clause)
      # print(query)
      result = session.run(query)
      results = []
      for record in result:
        final_record = { 
          "subject": record["subject"], 
          "site": record["site"],
        }
        results.append(final_record)
    result = {'items': results, 'page': page, 'size': size, 'filter': filter, 'count': count }
    return result
