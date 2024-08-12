from uuid import uuid4
from d4kms_generic import application_logger
from d4kms_service import Neo4jConnection
from model.base_node import Node
from model.code import Code
from model.alias_code import AliasCode
from model.biomedical_concept import BiomedicalConcept
from model.biomedical_concept_property import BiomedicalConceptProperty
from model.biomedical_concept import BiomedicalConceptSimple
from model.response_code import ResponseCode
from model.crm import CRMNode

class StudyDesignBC():

  @classmethod
  def fix(cls, name):
    results = {}
    study_design = cls._get_study_design(name)
    bcs = cls._get_bcs(study_design)
    for bc in bcs:
      # print("--debug bc:",bc.name)
      results[bc.name] = cls._add_property(bc)
      # application_logger.info(f"Inserted --DTC for '{bc.name}'")   
    return results
  
  @classmethod
  def create(cls, name):
    results = {}
    study_design = cls._get_study_design(name)
    properties = cls._get_properties(study_design)
    crm_nodes = cls._get_crm()
    crm_map = {}
    for uri, node in crm_nodes.items():
      vars = node.sdtm.split(',')
      for var in vars:
        if var not in crm_map:
          crm_map[var] = []  
        crm_map[var].append(node)
    #print(f"MAP {crm_map}")
    #print(f"PROPERTIES: {[i.name for i in properties]}")
    for p in properties:
      nodes = cls._match(p.name, crm_map)
      #print(f"N: {nodes}")
      if nodes:
        if len(nodes) == 1:
          node = nodes[0]
        else:
          if p.name.endswith('ORRES') and p.responseCodes:
            node = next((i for i in nodes if i.datatype == 'coded'), None)
          else:
            node = next((i for i in nodes if i.datatype == 'quantity'), None)
        if node:
          p.relationship(nodes[0], 'IS_A_REL')                
          application_logger.info(f"Linked BC property '{p.name}' -> '{node.uri}'")   
          results[p.name] = node.uri
        else:
          application_logger.error(f"Failed to link BC property '{p.name}', nodes '{nodes}' detected but no match")
      else:
        application_logger.error(f"Failed to link BC property '{p.name}', no nodes detected")
    return results

  @classmethod
  def unlinked(cls, uuid, page, size, filter):
    skip_offset_clause = ""
    if page != 0:
      offset = (page - 1) * size
      skip_offset_clause = "SKIP %s LIMIT %s" % (offset, size)
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[]->(bc:BiomedicalConcept) WHERE NOT (bc)<-[:USING_BC_REL]-()
        RETURN COUNT(DISTINCT(bc.name)) AS count
      """ % (uuid)
      #print(f"QUERY: {query}")
      result = session.run(query)
      count = 0
      for record in result:
        count = record['count']
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[]->(bc:BiomedicalConcept) WHERE NOT (bc)<-[:USING_BC_REL]-()
        RETURN DISTINCT(bc.name) as name ORDER BY name %s
      """ % (uuid, skip_offset_clause)
      #print(f"QUERY: {query}")
      result = session.run(query)
      results = []
      for record in result:
        results.append(record['name'])
    result = {'items': results, 'page': page, 'size': size, 'filter': filter, 'count': count }
    return result
  
  @classmethod
  def make_dob_surrogate_as_bc(cls, name):
    bc_uuid = cls._copy_surrogate()
    # print("bc_uuid",bc_uuid)
    bcp_uuids = cls._copy_properties(bc_uuid, 'Date of Birth', 'Race')
    # print("bcp_uuids",bcp_uuids)
    cls._copy_bc_relationships_from_bc(bc_uuid, 'Race')
    application_logger.info("Converted Date of Birth Surrgate to BC")   

  @classmethod
  def remove_properties_from_exposure(cls, name):
    cls._remove_properties_from_exposure()
    application_logger.info("Removed properties from exposure")   

  @classmethod
  def fix_links_to_crm(cls, name):
    cls._add_missing_links_to_crm()
    application_logger.info("Linked BRTHDTC to CRM")   



  @staticmethod
  def _match(name, map):
    name_upper = name.upper()
    generic_name = f"--{name[2:]}".upper()
    # if name_upper == "SEX":
    #   print(f"Match {name} {generic_name}")
    #   print(f"keys {map.keys()}")
    if name_upper in map.keys():
      #print(f"Matched full {map[name].uri}")
      return map[name_upper]
    elif generic_name in map.keys():
      #print(f"Matched generic {map[generic_name].uri}")
      return map[generic_name]
    return None

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
  def _get_properties(study_design):
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[:BIOMEDICAL_CONCEPTS_REL]->(bc:BiomedicalConcept)-[]->(bcp:BiomedicalConceptProperty)
        WITH bcp
        OPTIONAL MATCH (bcp)-[]->(rc:ResponseCode)-[]->(c:Code) 
        RETURN DISTINCT bcp, rc, c
      """ % (study_design.uuid)
      result = session.run(query)
      p_map = {}
      for record in result:
        if record['rc']:
          rc_params = Node.as_dict(record['rc'])
          rc_params['code'] = Code.wrap(record['c'])
          rc = ResponseCode.wrap(rc_params)
        else:
          rc = None
        p_params = Node.as_dict(record['bcp'])
        uuid = p_params['uuid']
        if uuid in p_map:
          p = p_map[uuid]
        else:
          code = Code(**{'uuid': 'uuid', 'id': '0', 'code': '1', 'codeSystem': '2', 'codeSystemVersion': '3', 'decode': '4', 'instanceType': 'Code'})
          p_params['code'] = AliasCode(**{'uuid': 'uuid', 'id': '0', 'standardCode': code, 'instanceType': 'AliasCode'})
          p = BiomedicalConceptProperty.wrap(p_params)
          p_map[uuid] = p
          results.append(p)
        if rc:
          p.responseCodes.append(rc)
      return results

  @staticmethod
  def _get_bcs(study_design):
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[:BIOMEDICAL_CONCEPTS_REL]->(bc:BiomedicalConcept)
        RETURN DISTINCT bc
      """ % (study_design.uuid)
      result = session.run(query)
      for record in result:
        results.append(BiomedicalConceptSimple.wrap(record['bc']))
      return results

  @staticmethod
  def _add_property(bc):
    uuids = {'property': str(uuid4()), 'code': str(uuid4()), 'aliasCode': str(uuid4())}
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (bc:BiomedicalConcept {uuid: '%s'})
        WITH bc
        CREATE (c:Code {uuid: $s_uuid1, id: 'tbd', code: 'tbd', codeSystem: 'http://www.cdisc.org', codeSystemVersion: '2023-09-29', decode: 'tbd', instanceType: 'Code'})
        CREATE (ac:AliasCode {uuid: $s_uuid2, id: 'tbd', instanceType: 'AliasCode'})
        CREATE (p:BiomedicalConceptProperty {uuid: $s_uuid3, id: 'tbd', name: '--DTC', label: 'Date Time', isRequired: 'true', isEnabled: 'true', datatype: 'datetime', instanceType: 'BiomedicalConceptProperty'})
        CREATE (bc)-[:PROPERTIES_REL]->(p)-[:CODE_REL]->(ac)-[:STANDARD_CODE_REL]->(c)
        RETURN p.uuid as uuid
      """ % (bc.uuid)
      result = session.run(query, 
        s_uuid1=str(uuid4()), 
        s_uuid2=str(uuid4()), 
        s_uuid3=str(uuid4())
      )
      for row in result:
        return uuids

  @staticmethod
  def _copy_surrogate():
    db = Neo4jConnection()
    bc_uuid = str(uuid4())

    with db.session() as session:
      results = []
      query = """
        MATCH (bcs:BiomedicalConceptSurrogate)
        where bcs.name = "Date of Birth"
        WITH bcs
        MERGE (bc:BiomedicalConcept {uuid:'%s'})
        SET bc.name         = bcs.name
        SET bc.description  = bcs.description
        SET bc.label        = bcs.label
        SET bc.name         = bcs.name
        SET bc.instanceType = "BiomedicalConcept"
        SET bc.reference    = "None set"
        SET bc.id           = "BiomedicalConcept_9999"
        SET bc.fake_node    = "yes"
        return bc.uuid as uuid
      """ % (bc_uuid)
      records = session.run(query)
      for record in records:
        results.append(record.data())
      if results:
        return results[0]['uuid']
      return "Error: Did not create BC"

  @staticmethod
  def _copy_properties(bc_uuid, new_bc_name, copy_bc_name):
    db = Neo4jConnection()
    bcp_uuids = []
    with db.session() as session:
      # Get properties for bc to copy
      query = """
          MATCH (bc:BiomedicalConcept {name:"%s"})-[:PROPERTIES_REL]->(bcp)
          RETURN bcp.uuid as uuid, bcp.name as name, bcp.label as label
      """ % (copy_bc_name)
      # print("query\n",query)
      # Create the same properties for new bc and add relationships to data contract and scheduled activity instance
      results = session.run(query)
      for bcp in [result.data() for result in results]:
          uuid = str(uuid4())

          # Create property node
          bcp_name = bcp['name'] if bcp['name'] != copy_bc_name else new_bc_name
          bcp_label = bcp['label'] if bcp['label'] != copy_bc_name else new_bc_name
          # HARD CODING: Mismatch in services databases
          if bcp_label == 'Date of Birth':
              bcp_label = 'Date/Time of Birth'
              bcp_name = 'BRTHDTC'
          query = """
              MATCH (source_bcp:BiomedicalConceptProperty {uuid:'%s'})
              with source_bcp
              MERGE (bcp:BiomedicalConceptProperty {uuid:'%s'})
              SET bcp.datatype     =	source_bcp.datatype
              SET bcp.id           =	'tbd'
              SET bcp.instanceType =	source_bcp.instanceType
              SET bcp.isEnabled    =	source_bcp.isEnabled
              SET bcp.isRequired   =	source_bcp.isRequired
              SET bcp.label        =	'%s'
              SET bcp.name         =	'%s'
              SET bcp.fake_node    = 'yes'
              RETURN bcp.uuid as uuid
          """ % (bcp['uuid'], uuid, bcp_label, bcp_name)
          # print("query",query)
          results = session.run(query)
          bcp_uuids.append([result.data() for result in results][0]['uuid'])
          # Link to new bc
          query = """
              MATCH (bc:BiomedicalConcept {uuid:'%s'})
              MATCH (bcp:BiomedicalConceptProperty {uuid:'%s'})
              MERGE (bc)-[r:PROPERTIES_REL]->(bcp)
              set r.fake_relationship = 'yes'
              RETURN 'done'
          """ % (bc_uuid, uuid)
          # print("query",query)
          results = session.run(query)
          print("Created link between BC and BCP",[result.data() for result in results])

          # # Copy property nodes relationships: DataContract
          # dc_uri="https://study.d4k.dk/study-cdisc-pilot-lzzt/"+bc_uuid+"/"+uuid
          # query = """
          #     MATCH (source_bcp:BiomedicalConceptProperty {uuid:'%s'})<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]-(sai:ScheduledActivityInstance)
          #     MATCH (bcp:BiomedicalConceptProperty {uuid:'%s'})
          #     with sai, bcp
          #     CREATE (dc:DataContract {uri:'%s', fake_node: 'yes'})
          #     CREATE (bcp)<-[r1:PROPERTIES_REL]-(dc)-[r2:INSTANCES_REL]->(sai)
          #     set r1.fake_relationship = 'yes'
          #     set r2.fake_relationship = 'yes'
          #     RETURN 'done'
          # """ % (bcp['uuid'], uuid, dc_uri)
          # print(query)
          # results = session.run(query)
          # for result in results:
          #   print("result",result.data())
          # print("Created Data Contract")

          # # Copy property nodes relationships: IS_A_REL        
          # query = """
          #     MATCH (source_bcp:BiomedicalConceptProperty {uuid:"%s"})-[:IS_A_REL]->(crm:CRMNode)
          #     MATCH (bcp:BiomedicalConceptProperty {uuid:"%s"})
          #     with crm, bcp
          #     MERGE (bcp)-[r:IS_A_REL]->(crm)
          #     set r.fake_relationship = "yes"
          #     return *
          # """ % (bcp['uuid'], uuid)
          # # print(query)
          # results = session.run(query)
          # for result in results:
          #   print("result",result.data())
          # print("Created property node IS_A_REL")
    return bcp_uuids
      
  @staticmethod
  def _copy_bc_relationships_from_bc(bc_uuid, copy_bc_name):
    db = Neo4jConnection()
    with db.session() as session:
      # Copy relationship to study
      query = """
          MATCH (copy_bc:BiomedicalConcept {name:"%s"})<-[:BIOMEDICAL_CONCEPTS_REL]-(target:StudyDesign)
          MATCH (new_bc:BiomedicalConcept {uuid:"%s"})
          MERGE (new_bc)<-[:BIOMEDICAL_CONCEPTS_REL]-(target)
      """ % (copy_bc_name, bc_uuid)
      # print(query)
      results = session.run(query)
      for result in results:
        print("result",result.data())
      print("Created link to Study Design")

      # Copy relationship to domain
      query = """
          MATCH (copy_bc:BiomedicalConcept {name:"%s"})<-[:USING_BC_REL]-(target:Domain)
          MATCH (new_bc:BiomedicalConcept {uuid:"%s"})
          MERGE (new_bc)<-[:USING_BC_REL]-(target)
      """ % (copy_bc_name, bc_uuid)
      # print(query)
      results = session.run(query)
      for result in results:
        print("result",result.data())
      print("Created link to Domain")

      # Copy relationship to activity
      query = """
          MATCH (copy_bc:BiomedicalConcept {name:"%s"})<-[:BIOMEDICAL_CONCEPT_REL]-(target:Activity)
          MATCH (new_bc:BiomedicalConcept {uuid:"%s"})
          MERGE (new_bc)<-[:BIOMEDICAL_CONCEPT_REL]-(target)
      """ % (copy_bc_name, bc_uuid)
      # print(query)
      results = session.run(query)
      for result in results:
        print("result",result.data())
      print("Created link to Activity")

      # Ignoring CODE_REL -> AliasCode

  @staticmethod
  def _add_missing_links_to_crm():
    db = Neo4jConnection()
    with db.session() as session:
      # If topic result (e.g. Date of Birth)
      # if bcp['name'] != copy_bc_name:
      # bcp_name = "Date of Birth"

      var_link_crm = {
        'BRTHDTC':'https://crm.d4k.dk/dataset/observation/observation_result/result/quantity/value'
       ,'RFICDTC':'https://crm.d4k.dk/dataset/common/period/period_start/date_time/value'
       ,'DSDECOD':'https://crm.d4k.dk/dataset/observation/observation_result/result/coding/code'
       ,'DSSTDTC':'https://crm.d4k.dk/dataset/common/period/period_start/date_time/value'
       ,'DSTERM' :'https://crm.d4k.dk/dataset/observation/observation_result/result/quantity/value'
       ,'VSPOS'  :'https://crm.d4k.dk/dataset/observation/position/coding/code'
       ,'DMDTC'  :'https://crm.d4k.dk/dataset/common/date_time/date_time/value'
       ,'EXDOSFRQ': 'https://crm.d4k.dk/dataset/therapeutic_intervention/frequency/coding/code'
       ,'EXROUTE': 'https://crm.d4k.dk/dataset/therapeutic_intervention/route/coding/code'
       ,'EXTRT': 'https://crm.d4k.dk/dataset/therapeutic_intervention/description/coding/code'
       ,'EXDOSFRM': 'https://crm.d4k.dk/dataset/therapeutic_intervention/form/coding/code'
       ,'EXDOSE': 'https://crm.d4k.dk/dataset/therapeutic_intervention/single_dose/quantity/value'
       ,'EXDOSU': 'https://crm.d4k.dk/dataset/therapeutic_intervention/single_dose/quantity/unit'
       ,'EXSTDTC': 'https://crm.d4k.dk/dataset/common/period/period_start/date_time/value'
       ,'EXENDTC': 'https://crm.d4k.dk/dataset/common/period/period_end/date_time/value'
      }

      for var,uri in var_link_crm.items():
        query = """
          MATCH (crm:CRMNode {uri:'%s'})
          MATCH (v:Variable {name:'%s'})
          with crm, v
          MERGE (v)-[r:IS_A_REL]->(crm)
          set r.fake_relationship = "yes"
          return "done" as done
        """ % (uri,var)
        # print("DS crm query",query)
        results = db.query(query)
        # print("crm query results",results)
        if results:
          application_logger.info(f"Created link to CRM from {var}")
        else:
          application_logger.info(f"Info: Failed to create link to CRM for {var}")
          print("query",query)

      # query = """
      #     // Create term and link to DSDECOD
      #     MERGE (t:SkosConcept {uri:'https://ct.d4k.dk/cdisc/dataset/sc/2014-03-28/sdtm/C114118/C16735'})
      #     SET t.pref_label = 'Informed Consent'
      #     SET t.identifier = 'C16735'
      #     SET t.alt_label = '[]'
      #     SET t.notation = 'INFORMED CONSENT OBTAINED'
      #     SET t.name = 'Informed Consent'
      #     SET t.definition = 'Consent by a patient for participation in a clinical study after achieving an understanding of the relevant medical facts and the risks involved.'
      #     SET t.id = 106651
      #     SET t.extensible = 'False'
      #     SET t.fake_node = 'yes'
      #     SET t.uuid = 'FakeUUIDInformedConcentObtained'
      #     with t
      #     MATCH (bcp:BiomedicalConceptProperty)-[:CODE_REL]->(:AliasCode)-[:STANDARD_CODE_REL]->(c:Code)
      #     WHERE bcp.name = 'DSDECOD'
      #     MERGE (c)-[:HAS_TERM]->(t)
      #     return bcp,c,t
      # """
      # # application_logger.info(f"BRTHDTC CRM query {query}")
      # results = db.query(query)
      # application_logger.info(f"Created term and link to DSDECOD {[result.data() for result in results]}")

  @staticmethod
  def _remove_properties_from_exposure():
    db = Neo4jConnection()
    with db.session() as session:
      # Just for simplifying life
      properties = ["EXREFID","EXLOC","EXFAST","EXDOSTXT","EXDOSRGM","EXDIR","EXLAT"]
      query = """
        MATCH (bc:BiomedicalConcept {name:'Exposure Unblinded'})-[:PROPERTIES_REL]->(p:BiomedicalConceptProperty)
        WHERE  p.name in %s
        DETACH DELETE p
        RETURN count(p)
      """ % (properties)
      # print("Delete Exposure Unblinded properties query",query)
      results = db.query(query)
      print("Delete exposure properties results",results)
      if results:
        application_logger.info(f"Removed {properties}")
      else:
        application_logger.info(f"Info: Failed to remove {properties}")
