from model.neo4j_connection import Neo4jConnection
from model.study_domain_instance import StudyDomainInstance

class StudyDesignSDTM():

  @classmethod
  def create(cls, uuid):
    pass

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
