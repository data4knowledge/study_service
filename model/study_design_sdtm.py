from uuid import uuid4
from d4kms_service import Neo4jConnection
from d4kms_generic import application_logger
from model.domain import Domain
from model.variable import Variable
from service.bc_service import BCService
from service.sdtm_service import SDTMService
from model.crm import CRMNode
from model.biomedical_concept import BiomedicalConceptSimple

class StudyDesignSDTM():

  @classmethod
  def create(cls, name):
    domains = {'DM': ['Age', 'Sex', 'Race', 'Ethnic']} # Need to fix that DM BCs have no SDTM representation yet.
    bc_service = BCService()
    sdtm_service = SDTMService()
    sdtm_bcs = bc_service.biomedical_concepts('sdtm', 1, 1000)
    #print(f"BCS: {sdtm_bcs}")
    crm_nodes = cls._get_crm()
    #print(f"CRM: {crm_nodes}")
    study_design = cls._get_study_design(name)
    #print(f"SD: {study_design.uuid}")
    results = cls._get_bcs(study_design)
    #print(f"BCs: {results}")
    for name in results:
      bc = next((item for item in sdtm_bcs['items'] if item["name"] == name), None)
      if bc:
        #print(f"RESULTS: {bc}")
        uuid = bc['uuid']
        response = bc_service.biomedical_concept('sdtm', uuid)
        if 'domain' in response:
          domain = response['domain']
          if domain not in domains:
            application_logger.info(f"Adding domain to load set '{domain}'")
            domains[domain] = []
          domains[domain].append(name)
    domains_metadata = sdtm_service.domains(1,100)
    #print(f"DOMAINS: {domains_metadata}")
    for domain, bcs in domains.items():
      domain_metadata = next((item for item in domains_metadata['items'] if item["name"] == domain), None)
      if domain_metadata:
        application_logger.info(f"Processing domain '{domain}'")
        domain_metadata = sdtm_service.domain(domain_metadata['uuid'])
        domain_metadata.pop('identified_by')
        domain_metadata.pop('has_status')
        domain_metadata.pop('bc_references')
        variables = domain_metadata.pop('items')
        domain_metadata['uuid'] = uuid4()
        d = Domain.create(domain_metadata)
        study_design.relationship(d, 'DOMAIN_REL')            
        for variable in variables:
          #print(f"VAR: {variable}")
          variable.pop('bc_references')
          crm_references = variable.pop('crm_references')
          # ----- Temporary fix
          variable['description'] = variable['description'].replace("'", "")
          # -----
          variable['uuid'] = uuid4()
          v = Variable.create(variable)
          d.relationship(v, 'VARIABLE_REL')            
          for ref in crm_references:
            #print(f"CRM REF: {ref}")
            if ref['uri_reference'] in crm_nodes:
              c = crm_nodes[ref['uri_reference']]
              v.relationship(c, 'IS_A_REL')                
              application_logger.info(f"Created CRM reference for variable '{v.name}' -> '{ref}'")
        for name in bcs:
          bc_set = cls._get_bcs_by_name(study_design, name)
          #print(f"BC SET: For {name} = {bc_set}")
          for bc in bc_set:
            d.relationship(bc, 'USING_BC_REL')                
            application_logger.info(f"Linked domain '{d.name}' -> '{bc.name}', '{bc.uuid}'")   
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
        MATCH (sd:StudyDesign {uuid: '%s'})-[]->(d:Domain) RETURN COUNT(d) AS count
      """ % (uuid)
      result = session.run(query)
      count = 0
      for record in result:
        count = record['count']
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[]->(d:Domain) RETURN d
        ORDER BY d.name %s
      """ % (uuid, skip_offset_clause)
      #print(query)
      result = session.run(query)
      results = []
      for record in result:
        results.append(StudyDesignDomain.wrap(record['d']).__dict__)
    result = {'items': results, 'page': page, 'size': size, 'filter': filter, 'count': count }
    return result

  @classmethod
  def domain(cls, uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (d:StudyDesignDomain {uuid: '%s'}) RETURN d
      """ % (uuid)
      result = session.run(query)
      for record in result:
        print("DOMAIN:", record['d']['name'])

  @staticmethod
  def _get_study_design(name):
    
    from model.study_design import StudyDesign
    
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign {name: '%s'}) return sd
      """ % (name)
      result = session.run(query)
      for record in result:
        return StudyDesign.wrap(record['sd'])
      return None

  @staticmethod
  def _get_bcs(study_design):
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[:BIOMEDICAL_CONCEPTS_REL]->(bc:BiomedicalConcept) RETURN DISTINCT bc.name as name
      """ % (study_design.uuid)
      result = session.run(query)
      for record in result:
        results.append(record['name'])
      return results

  @staticmethod
  def _get_bcs_by_name(study_design, name):
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[:BIOMEDICAL_CONCEPTS_REL]->(bc:BiomedicalConcept) WHERE bc.name = '%s' RETURN DISTINCT bc
      """ % (study_design.uuid, name)
      result = session.run(query)
      for record in result:
        results.append(BiomedicalConceptSimple.wrap(record['bc']))
      return results

  @staticmethod
  def _get_crm():
    db = Neo4jConnection()
    with db.session() as session:
      results = {}
      query = """
        MATCH (n:CRMNode) return n
      """ 
      records = session.run(query)
      for record in records:
        crm_node = CRMNode.wrap(record['n'])
        results[crm_node.uri] = crm_node
      return results
