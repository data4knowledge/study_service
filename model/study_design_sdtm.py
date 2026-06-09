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
  def create(cls, sd_uuid):
    domains = {'DM': ['Age', 'Sex', 'Race', 'Ethnic','Informed Consent Obtained','Date of Birth']} # Need to fix that DM BCs have no SDTM representation yet.
    bc_service = BCService()
    sdtm_service = SDTMService()
    sdtm_bcs = bc_service.biomedical_concepts('sdtm', 1, 1000)
    #print(f"BCS: {sdtm_bcs}")
    crm_nodes = cls._get_crm()
    #print(f"CRM: {crm_nodes}")
    study_design = cls._get_study_design_by_uuid(sd_uuid)
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
        results.append(Domain.wrap(record['d']).__dict__)
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

  @classmethod
  def add_permissible_sdtm_variables(cls, sd_uuid):
    study_design = cls._get_study_design_by_uuid(sd_uuid)
    cls._add_permissible_sdtm_variables(study_design.uuid)
    application_logger.info("Created permissible variables")

  # @classmethod
  # def add_links_to_sdtm(cls, name):
  #   study_design = cls._get_study_design(name)
  #   domain_uuid = cls._find_domain(study_design.uuid, 'DS')
  #   domain = Domain.find(domain_uuid)
  #   bc_set = cls._get_bcs_by_name(study_design, "Exposure Unblinded")
  #   for bc in bc_set:
  #     print("bc",bc)
  #     domain.relationship(bc, 'USING_BC_REL')
  #     application_logger.info(f"(Hard coded) Linked domain '{domain.name}' -> '{bc.name}', '{bc.uuid}'")   

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
  def _get_study_design_by_uuid(sd_uuid):

    from model.study_design import StudyDesign
    return StudyDesign.find(sd_uuid)

  @staticmethod
  def _find_domain(study_design_uuid, domain):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[:DOMAIN_REL]->(domain:Domain)
        WHERE domain.name = '%s'
        return domain.uuid as uuid
      """ % (study_design_uuid, domain)
      print("-- find query", query)
      results = session.run(query)
      for record in results:
        return record['uuid']
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

  @staticmethod
  def _add_permissible_sdtm_variables(study_design_uuid):
    order_of_timing_variables = [
        'VISITNUM'
        ,'VISIT'
        ,'VISITDY'
        ,'TAETORD'
        ,'EPOCH'
        ,'RPHASE'
        ,'RPPLDY'
        ,'RPPLSTDY'
        ,'RPPLENDY'
        ,'--DTC'
        ,'--STDTC'
        ,'--ENDTC'
        ,'--DY'
        ,'--STDY'
        ,'--ENDY'
        ,'--NOMDY'
        ,'--NOMLBL'
        ,'--RPDY'
        ,'--RPSTDY'
        ,'--RPENDY'
        ,'--XDY'
        ,'--XSTDY'
        ,'--XENDY'
        ,'--CHDY'
        ,'--CHSTDY'
        ,'--CHENDY'
        ,'--DUR'
        ,'--TPT'
        ,'--TPTNUM'
        ,'--ELTM'
        ,'--TPTREF'
        ,'--RFTDTC'
        ,'--STRF'
        ,'--ENRF'
        ,'--EVLINT'
        ,'--EVINTX'
        ,'--STRTPT'
        ,'--STTPT'
        ,'--ENRTPT'
        ,'--ENTPT'
        ,'MIDS'
        ,'RELMIDS'
        ,'MIDSDTC'
        ,'--STINT'
        ,'--ENINT'
        ,'--DETECT'
        ,'--PTFL'
        ,'--PDUR'
    ]
    permissible = [
      {'domain':'EX','name': 'VISIT','uri': 'https://sdtm.d4k.dk/ig/EX/VISIT','uuid': str(uuid4()),'code_list': '','code_list_uri': '','core': 'Perm','data_type': 'Char','description': 'See IG','label': 'Visit Name','ordinal': 48,'role': 'Timing','value_domain': ''},
      {'domain':'EX','name': 'VISITNUM','uri': 'https://sdtm.d4k.dk/ig/EX/VISITNUM','uuid': str(uuid4()),'code_list': '','code_list_uri': '','core': 'Perm','data_type': 'Char','description': 'See IG','label': 'Visit Name','ordinal': 55,'role': 'Timing','value_domain': ''}
    ]

    db = Neo4jConnection()
    with db.session() as session:
      for domain in set([var['domain'] for var in permissible]):
        query = """
          MATCH (sd:StudyDesign {uuid: '%s'})-[:DOMAIN_REL]->(domain:Domain)
          WHERE domain.name = '%s'
          return domain.uuid as uuid
        """ % (study_design_uuid, domain)
        results = db.query(query)
        if results:
          d = Domain.find(results[0]['uuid'])
          for variable in [v for v in permissible if v['domain'] == domain]:
            variable.pop('domain')
            v = Variable.create(variable)
            d.relationship(v, 'VARIABLE_REL')
            application_logger.info(f"---  Info permissible variables: Added variable {domain}.{variable['name']}")
        else:
          application_logger.info(f"---  Info permissible variables: Failed to find domain {domain}")

      # # Order variables
      # for domain in set([var['domain'] for var in permissible]):
      #   query = """
      #     MATCH (sd:Domain {uuid: '%s'})-[:VARIABLE_REL]->(sv:Variable)
      #     WHERE sd.name = "%s"
      #     RETURN sv.ordinal as ordinal, sv.name as name ORDER BY toInteger(sv.ordinal)
      #   """ % (study_design_uuid, domain)
      #   print("-- st query",query)
      #   results = db.query(query)
