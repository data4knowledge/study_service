from d4kms_service import Neo4jConnection
from model.study_domain_instance import StudyDomainInstance
from model.biomedical_concept import BiomedicalConcept
from service.bc_service import BCService

class StudyDesignSDTM():

  @classmethod
  async def create(cls, uuid):
    domains = {}
    bc_service = BCService()
    sdtm_bcs = await bc_service.biomedical_concepts('sdtm', 1, 1000)
    #print(f"BCS: {sdtm_bcs}")
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[:BIOMEDICAL_CONCEPTS_REL]->(bc:BiomedicalConcept) RETURN DISTINCT bc.name as name
      """ % (uuid)
      result = session.run(query)
      results = []
      for record in result:
        results.append(record['name'])
      for name in results:
        bc = next((item for item in sdtm_bcs['items'] if item["name"] == name), None)
        if bc:
          #print(f"RESULTS: {bc}")
          uuid = bc['uuid']
          response = await bc_service.biomedical_concept('sdtm', uuid)
          if 'domain' in response:
            domain = response['domain']
            if domain not in domains:
              domains[domain] = []
            domains[domain].append(name)
          #print(f"RESPONSE: {response}")
    return {'results': domains}
  
  @classmethod
  def domains(cls, uuid, page, size, filter):
    skip_offset_clause = ""
    if page != 0:
      offset = (page - 1) * size
      skip_offset_clause = "SKIP %s LIMIT %s" % (offset, size)
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[]->(d:StudyDomainInstance) RETURN COUNT(d) AS count
      """ % (uuid)
      result = session.run(query)
      count = 0
      for record in result:
        count = record['count']
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[]->(d:StudyDomainInstance) RETURN d
        ORDER BY d.name %s
      """ % (uuid, skip_offset_clause)
      #print(query)
      result = session.run(query)
      results = []
      for record in result:
        results.append(StudyDomainInstance.wrap(record['d']).__dict__)
    result = {'items': results, 'page': page, 'size': size, 'filter': filter, 'count': count }
    return result

  @classmethod
  def domain(cls, uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (d:StudyDomainInstance {uuid: '%s'}) RETURN d
      """ % (uuid)
      result = session.run(query)
      for record in result:
        print("DOMAIN:", record['d']['name'])
