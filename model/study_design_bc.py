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
  def fix(cls, sd_uuid):
    results = {}
    study_design = cls._get_study_design_by_uuid(sd_uuid)
    bcs = cls._get_bcs(study_design)
    for bc in bcs:
      # print("--debug bc:",bc.name)
      results[bc.name] = cls._add_property(bc, '--DTC', 'Collection date')
      # application_logger.info(f"Inserted --DTC for '{bc.name}'")   
    return results

  @classmethod
  def get_bcs_and_properties(cls, uuid):
    study_design = cls._get_study_design_by_uuid(uuid)
    bcs = cls._get_bcs_and_properties(study_design)
    return bcs

  @classmethod
  def get_lab_transfer_spec(cls, uuid):
    study_design = cls._get_study_design_by_uuid(uuid)
    bcs = cls._get_lab_transfer_spec(study_design)
    return bcs

  @classmethod
  def get_activities_by_visit(cls, uuid, page, size, filter):
    study_design = cls._get_study_design_by_uuid(uuid)
    bcs = cls._get_activities_by_visit(study_design, page, size, filter)
    result = {'items': bcs, 'page': page, 'size': size, 'filter': filter, 'count': 1 }
    return result

  @classmethod
  def get_visits(cls, uuid):
    visits = cls._get_visits(uuid)
    return visits

  @classmethod
  def get_datapoint_stuff(cls, dp_uri):
    data = cls._get_datapoint_stuff(dp_uri)
    return data

  @classmethod
  def get_datapoint_valid_values(cls, dp_uri):
    valid_values = cls._get_bc_properties(dp_uri)
    return valid_values

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
  def make_dob_surrogate_as_bc(cls, sd_uuid):
    bc_uuid = cls._copy_surrogate(sd_uuid)
    if bc_uuid:
      # print("bc_uuid",bc_uuid)
      bcp_uuids = cls._copy_properties(sd_uuid, bc_uuid, 'Date of Birth', 'Race')
      # print("bcp_uuids",bcp_uuids)
      cls._copy_bc_relationships_from_bc(sd_uuid, bc_uuid, 'Race')
      application_logger.info("Converted Date of Birth Surrogate to BC")
    else:
      application_logger.info("Date of Birth Surrogate not found")

  @classmethod
  def pretty_properties_for_bc(cls, sd_uuid):
    cls._remove_properties_from_exposure(sd_uuid)

    bcs = cls._get_bcs_by_name(sd_uuid, "Adverse Event Prespecified")
    cls._add_properties_to_ae(bcs)
    application_logger.info("Added properties to AE")

  @classmethod
  def fix_links_to_crm(cls, name):
    cls._add_missing_links_to_crm()
    application_logger.info("Linked specific variables to CRM")

  @classmethod
  def fix_bc_name_label(cls, name):
    cls._fix_bc_name_label()
    cls._add_missing_terminology()
    cls._add_brthdtc_link_crm()
    application_logger.info("Added alt_sdtm_name and terms")



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
  def _get_study_design_by_uuid(uuid):
    
    from model.study_design import StudyDesign
    
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'}) return sd
      """ % (uuid)
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
      # print("query", query)
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
  def _get_bcs_and_properties(study_design):
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      # Get BCPs without VLM
      query = """
        // Find BC's used
        MATCH (sd:StudyDesign {uuid: '%s'})-[:BIOMEDICAL_CONCEPTS_REL]->(bc:BiomedicalConcept)
        WITH distinct bc.name as bc_name
        WITH collect(bc_name) as names
        unwind names as name
        // Get only one match per name
        CALL {
          WITH name
          MATCH (bc:BiomedicalConcept)
          WHERE bc.name = name
          return bc
          limit 1
        }
        WITH bc
        MATCH (bc)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(cd:Code)
        MATCH (bc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)
        WHERE NOT EXISTS { 
          (bcp)-[:RESPONSE_CODES_REL]->(:ResponseCode)-[:CODE_REL]->(:Code)
        }
        WITH bc, bcp, cd, crm
        optional match (bcp)-[:DATA_ENTRY_CONFIG]-(dec:DataEntryConfig)
        OPTIONAL MATCH (d:Domain)-[:USING_BC_REL]->(bc)
        OPTIONAL MATCH (crm)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(d)
        where bcp.name = var.name or bcp.label = var.label or bcp.alt_sdtm_name = var.name
        WITH distinct bc.name as bc_raw_name, cd.decode as bc_name, bcp.name as name, bcp.generic_name as generic_name, bcp.datatype as bcp_datatype, dec.question_text as question_text, crm.datatype as data_type, d.name as domain, d.label as domain_label, var.name as variable, "" as code, "" as pref_label, "" as notation
        return "no code" as from, bc_raw_name, bc_name, bcp_datatype, name, generic_name, question_text, data_type, collect({domain:domain,label:domain_label,variable:variable}) as sdtm, [] as terms
      """ % (study_design.uuid)
        # return from, bc_raw_name, bc_name, name, data_type, sdtm, terms
      # print("bc-prop query", query)
      result = session.run(query)
      for record in result:
        # results.append(record['bc'].data())
        results.append(record.data())
        # results.append(BiomedicalConceptSimple.wrap(record['bc']))

      # Get BCPs with VLM
      query = """
        // Find BC's used
        MATCH (sd:StudyDesign {uuid: '%s'})-[:BIOMEDICAL_CONCEPTS_REL]->(bc:BiomedicalConcept)
        WITH distinct bc.name as bc_name
        WITH collect(bc_name) as names
        unwind names as name
        // Get only one match per name
        CALL {
          WITH name
          MATCH (bc:BiomedicalConcept)
          WHERE bc.name = name
          return bc.uuid as bc_uuid
          limit 1
        }
        WITH bc_uuid
        MATCH (bc)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(cd:Code)
        where bc.uuid = bc_uuid
        MATCH (bc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)-[:RESPONSE_CODES_REL]->(rc:ResponseCode)-[:CODE_REL]->(c:Code)
        MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)
        optional match (bcp)-[:DATA_ENTRY_CONFIG]-(dec:DataEntryConfig)
        MATCH (d:Domain)-[:USING_BC_REL]->(bc)
        // OPTIONAL MATCH (crm)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(d)
        MATCH (crm)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(d)
        where bcp.name = var.name or bcp.label = var.label
        WITH distinct bc.name as bc_raw_name, cd.decode as bc_name, bcp.name as name, bcp.generic_name as generic_name, bcp.datatype as bcp_datatype, dec.question_text as question_text, crm.datatype as data_type, d.name as domain, d.label as domain_label, var.name as variable, c.code as code, c.decode as pref_label, c.decode as notation
        return "second" as from, bc_raw_name, bc_name, bcp_datatype, name, generic_name, question_text, data_type, collect({domain:domain,label:domain_label,variable:variable}) as sdtm, collect({code:code,pref_label:pref_label,notation:notation}) as terms
      """ % (study_design.uuid)
        # return from, bc_raw_name, bc_name, name, data_type, sdtm, terms
      # print("bc-vlm query", query)
      result = session.run(query)
      for record in result:
        # results.append(record['bc'].data())
        results.append(record.data())
        # results.append(BiomedicalConceptSimple.wrap(record['bc']))

    db.close()
    return results

  @staticmethod
  def _get_bc_properties(datapoint):
    db = Neo4jConnection()
    with db.session() as session:
      # results = []
      results = {}
      # Get BCPs without VLM
      # query = """
      #   match (dp:DataPoint {uri:'%s'})
      #   match (dp)-[:FOR_SUBJECT_REL]->(subj:Subject)
      #   match (dp)-[:FOR_DC_REL]->(dc0:DataContract)-[:PROPERTIES_REL]->(bcp0:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(cd:Code)
      #   with dc0, bc, subj, cd
      #   match (dc0)-[:INSTANCES_REL]->(main_sai:ScheduledActivityInstance)<-[:INSTANCES_REL]-(:ScheduleTimeline {mainTimeline: 'True'})
      #   match (main_sai)-[:ENCOUNTER_REL]-(enc:Encounter)
      #   match (dc0)-[:INSTANCES_REL]->(sub_sai:ScheduledActivityInstance)<-[:INSTANCES_REL]-(x:ScheduleTimeline {mainTimeline: 'False'})
      #   match (sub_sai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(timing:Timing)
      #   match (bc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(dc)
      #   match (bcp)-[:IS_A_REL]->(crm:CRMNode)
      #   optional match (bcp)-[:DATA_ENTRY_CONFIG]-(dec:DataEntryConfig)
      #   match (dc)-[:INSTANCES_REL]->(main_sai)
      #   match (dc)-[:INSTANCES_REL]->(sub_sai)
      #   optional match (dc)<-[:FOR_DC_REL]-(dp:DataPoint)-[:FOR_SUBJECT_REL]->(subj)
      #   OPTIONAL MATCH (d:Domain)-[:USING_BC_REL]->(bc)
      #   OPTIONAL MATCH (crm)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(d)
      #   where bcp.name = var.name or bcp.label = var.label or bcp.alt_sdtm_name = var.name
      #   WITH distinct subj.identifier as subj_id, enc.label as visit, timing.value as tpt, dec.question_text as question_text, bc.name as bc_raw_name, cd.decode as bc_name, bcp.name as name, crm.datatype as data_type, dp.value as value, dp.uri as dp_uri, d.name as domain, d.label as domain_label, var.name as variable, "" as code, "" as pref_label, "" as notation
      #   return "sub" as from, subj_id, visit, tpt, question_text, bc_raw_name, bc_name, name, data_type, collect({value:value, uri:dp_uri}) as dp_values, collect({domain:domain,label:domain_label,variable:variable}) as sdtm, [] as terms
      # """ % (datapoint)
      # print("get_bc valid sub-timeline  query", query)
      # result = session.run(query)
      # for record in result:
      #   results.append(record.data())


      # Get BCPs with VLM
      query = """
          match (dp:DataPoint {uri:'%s'})
          match (dp)-[:FOR_SUBJECT_REL]->(subj:Subject)
          match (dp)-[:FOR_DC_REL]->(dc0:DataContract)-[:PROPERTIES_REL]->(bcp0:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(cd:Code)
          with dc0, bc, cd
          match (dc0)-[:INSTANCES_REL]->(main_sai:ScheduledActivityInstance)<-[:INSTANCES_REL]-(:ScheduleTimeline {mainTimeline: 'True'})
          match (main_sai)-[:ENCOUNTER_REL]-(enc:Encounter)
          match (bc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(dc)
          MATCH (bc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)-[:RESPONSE_CODES_REL]->(rc:ResponseCode)-[:CODE_REL]->(c:Code)
          match (bcp)-[:IS_A_REL]->(crm:CRMNode)
          match (dc)-[:INSTANCES_REL]->(main_sai)
          WITH distinct bc.name as bc_raw_name, cd.decode as bc_name, bcp.name as name, crm.datatype as data_type, c.code as code, c.pref_label as pref_label, c.notation as notation
          return bc_raw_name, bc_name, name, data_type, collect(notation) as terms
        """ % (datapoint)
      # print("get_bc valid main-timeline  query", query)
      result = session.run(query)
      for record in result:
        # results.append(record.data())
        results[record['name']] = record['terms']
    db.close()
    return results

  @staticmethod
  def _get_lab_transfer_spec(study_design):
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      # Get BCs with source 'lab'
      query = """
        MATCH (st:ScheduleTimeline)<-[]-(sd:StudyDesign {uuid: '%s'})-[]->(e1:Encounter)
        WHERE NOT (e1)-[:PREVIOUS_REL]->()
        WITH e1
        MATCH path=(e1)-[:NEXT_REL *0..]->(e)
        WITH e, LENGTH(path) as order
        MATCH (e)<-[:ENCOUNTER_REL]-(sai:ScheduledActivityInstance)
        MATCH (sai)<-[:INSTANCES_REL]-(dc:DataContract)-[PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
        MATCH (bc)<-[:USING_BC_REL]->(:Domain {name:'LB'})
        MATCH (bcp)-[:CODE_REL]-(ac:AliasCode)-[:STANDARD_CODE_REL]-(sc:Code)
        MATCH (bcp)-[:IS_A_REL]-(crm:CRMNode)
        MATCH (bcp)<-[:PROPERTIES_REL]-(dc:DataContract)
        optional MATCH (bcp)-[:RESPONSE_CODES_REL]->(:ResponseCode)-[:CODE_REL]->(c:Code)
        with distinct order, e.label as encounter, bc.name as name, {id: sc.code, decode: sc.decode} as coded_name, bcp.generic_name as bcp_name, c.decode as term, dc.uri as dc
        return order, encounter, name, coded_name, bcp_name, collect(term) as terms, dc
        order by order, name, bcp_name
      """ % (study_design.uuid)
      # print("lab transfer query", query)
      result = session.run(query)
      for record in result:
        results.append(record.data())


    db.close()
    return results

  @staticmethod
  def _get_activities_by_visit(study_design, page, size, filter):
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      # Get BCs with source 'lab'
      query = """
        MATCH (st:ScheduleTimeline)<-[]-(sd:StudyDesign {uuid: '%s'})-[]->(a1:Activity)
        WHERE NOT (a1)-[:PREVIOUS_REL]->()
        WITH a1 
        MATCH path=(a1)-[:NEXT_REL *0..]->(a)
        WITH a, LENGTH(path) as a_ord
        MATCH (a)<-[:ACTIVITY_REL]-(sai:ScheduledActivityInstance)-[:ENCOUNTER_REL]->(enc:Encounter)
        optional MATCH (a)<-[:CHILD_REL]-(p:Activity)       
        optional match (a)-[:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)<-[:USING_BC_REL]-(d:Domain)
        with distinct toInteger(split(enc.id,'_')[1]) as order, a_ord, enc.label as visit, p.name as parent, a.label as activity, bc.name as bc_name
        return order, a_ord, visit, parent, activity, collect(bc_name) as bcs
        order by order, a_ord
      """ % (study_design.uuid)
      # print("activity by visit query", query)
      result = session.run(query)
      for record in result:
        results.append(record.data())


    db.close()
    return results

  @staticmethod
  def _get_visits(uuid):
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[:BIOMEDICAL_CONCEPTS_REL]->(bc:BiomedicalConcept)
        MATCH (bc)-[:PROPERTIES_REL]->(bcp)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst)-[:ENCOUNTER_REL]-(enc)
        WITH distinct bc.name as bc_name, enc.label as label, enc.name as visit, enc.description as description, toInteger(split(enc.id,'_')[1]) as visitnum
        order by bc_name, visitnum
        // return bc_name, collect({label:label, visit:visit, description:description, visitnum:visitnum}) as visits
        return bc_name, collect(label) as visits
      """ % (uuid)
      # print("get-visits query", query)
      result = session.run(query)
      for record in result:
        # results.append(record['bc'].data())
        results.append(record.data())
        # results.append(BiomedicalConceptSimple.wrap(record['bc']))
      return results

  @staticmethod
  def _get_datapoint_stuff(dp_uri):
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      # Get bcp when on sub-timeline
      query = """
        match (dp:DataPoint {uri:'%s'})
        match (dp)-[:FOR_SUBJECT_REL]->(subj:Subject)
        match (dp)-[:FOR_DC_REL]->(dc0:DataContract)-[:PROPERTIES_REL]->(bcp0:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(cd:Code)
        with dc0, bc, subj, cd
        match (dc0)-[:INSTANCES_REL]->(main_sai:ScheduledActivityInstance)<-[:INSTANCES_REL]-(:ScheduleTimeline {mainTimeline: 'True'})
        match (main_sai)-[:ENCOUNTER_REL]-(enc:Encounter)
        match (dc0)-[:INSTANCES_REL]->(sub_sai:ScheduledActivityInstance)<-[:INSTANCES_REL]-(x:ScheduleTimeline {mainTimeline: 'False'})
        match (sub_sai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(timing:Timing)
        match (bc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(dc)
        match (bcp)-[:IS_A_REL]->(crm:CRMNode)
        optional match (bcp)-[:DATA_ENTRY_CONFIG]-(dec:DataEntryConfig)
        match (dc)-[:INSTANCES_REL]->(main_sai)
        match (dc)-[:INSTANCES_REL]->(sub_sai)
        optional match (dc)<-[:FOR_DC_REL]-(dp:DataPoint)-[:FOR_SUBJECT_REL]->(subj)
        OPTIONAL MATCH (d:Domain)-[:USING_BC_REL]->(bc)
        OPTIONAL MATCH (crm)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(d)
        where bcp.name = var.name or bcp.label = var.label or bcp.alt_sdtm_name = var.name
        WITH distinct subj.identifier as subj_id, enc.label as visit, timing.value as tpt, dec.question_text as question_text, bc.name as bc_raw_name, cd.decode as bc_name, bcp.name as name, bcp.generic_name as generic_name, crm.datatype as data_type, dp.value as value, dp.uri as dp_uri, d.name as domain, d.label as domain_label, var.name as variable, "" as code, "" as pref_label, "" as notation
        return "sub" as from, subj_id, visit, tpt, question_text, bc_raw_name, bc_name, name, generic_name, data_type, collect({value:value, uri:dp_uri}) as dp_values, collect({domain:domain,label:domain_label,variable:variable}) as sdtm, [] as terms
      """ % (dp_uri)
      print("get_datapoint sub-timeline  query", query)
      result = session.run(query)
      for record in result:
        results.append(record.data())

      #  NOTE: Result will be empty if it is not on sub-timeline. Get from main-timeline
      if not results:
        # Get bcp when on main-timeline
        query = """
          match (dp:DataPoint {uri:'%s'})
          match (dp)-[:FOR_SUBJECT_REL]->(subj:Subject)
          match (dp)-[:FOR_DC_REL]->(dc0:DataContract)-[:PROPERTIES_REL]->(bcp0:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(cd:Code)
          with dc0, bc, subj, cd
          match (dc0)-[:INSTANCES_REL]->(main_sai:ScheduledActivityInstance)<-[:INSTANCES_REL]-(:ScheduleTimeline {mainTimeline: 'True'})
          match (main_sai)-[:ENCOUNTER_REL]-(enc:Encounter)
          match (bc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(dc)
          match (bcp)-[:IS_A_REL]->(crm:CRMNode)
          optional match (bcp)-[:DATA_ENTRY_CONFIG]-(dec:DataEntryConfig)
          match (dc)-[:INSTANCES_REL]->(main_sai)
          optional match (dc)<-[:FOR_DC_REL]-(dp:DataPoint)-[:FOR_SUBJECT_REL]->(subj)
          OPTIONAL MATCH (d:Domain)-[:USING_BC_REL]->(bc)
          OPTIONAL MATCH (crm)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(d)
          where bcp.name = var.name or bcp.label = var.label or bcp.alt_sdtm_name = var.name
          WITH distinct subj.identifier as subj_id, enc.label as visit, dec.question_text as question_text, bc.name as bc_raw_name, cd.decode as bc_name, bcp.name as name, bcp.generic_name as generic_name, crm.datatype as data_type, dp.value as value, dp.uri as dp_uri, d.name as domain, d.label as domain_label, var.name as variable, "" as code, "" as pref_label, "" as notation
          return "main" as from, subj_id, visit, question_text, bc_raw_name, bc_name, name, generic_name, data_type, collect({value:value, uri:dp_uri}) as dp_values, collect({domain:domain,label:domain_label,variable:variable}) as sdtm, [] as terms
        """ % (dp_uri)
        print("get_datapoint main-timeline  query", query)
        result = session.run(query)
        for record in result:
          results.append(record.data())

      # NOTE: Result will be empty if it is not linked to a timing/encounter or unscheduled. E.g. AE
      if not results:
        # Get bcp when on main-timeline
        query = """
          match (dp:DataPoint {uri:'%s'})
          match (dp)-[:FOR_SUBJECT_REL]->(subj:Subject)
          match (dp)-[:SOURCE]->(sr:Record)
          match (dp)-[:FOR_DC_REL]->(dc0:DataContract)-[:PROPERTIES_REL]->(bcp0:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(cd:Code)
          with dc0, bc, subj, cd, sr
          match (dc0)-[:INSTANCES_REL]->(sub_sai:ScheduledActivityInstance)<-[:INSTANCES_REL]-(x:ScheduleTimeline)
          // match (sub_sai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(timing:Timing)
          match (bc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(dc)
          match (bcp)-[:IS_A_REL]->(crm:CRMNode)
          optional match (bcp)-[:DATA_ENTRY_CONFIG]-(dec:DataEntryConfig)
          // match (dc)-[:INSTANCES_REL]->(main_sai)
          // match (dc)-[:INSTANCES_REL]->(sub_sai)
          match (dc)<-[:FOR_DC_REL]-(dp:DataPoint)-[:FOR_SUBJECT_REL]->(subj)
          match (dp)-[:SOURCE]->(sr)
          OPTIONAL MATCH (d:Domain)-[:USING_BC_REL]->(bc)
          OPTIONAL MATCH (crm)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(d)
          where bcp.name = var.name or bcp.label = var.label or bcp.alt_sdtm_name = var.name
          WITH distinct subj.identifier as subj_id, "N/A" as visit, "N/A" as tpt, dec.question_text as question_text, bc.name as bc_raw_name, cd.decode as bc_name, bcp.name as name, bcp.generic_name as generic_name, crm.datatype as data_type, dp.value as value, dp.uri as dp_uri, d.name as domain, d.label as domain_label, var.name as variable, "" as code, "" as pref_label, "" as notation
          return "sub2" as from, subj_id, visit, tpt, question_text, bc_raw_name, bc_name, name, generic_name, data_type, collect({value:value, uri:dp_uri}) as dp_values, collect({domain:domain,label:domain_label,variable:variable}) as sdtm, [] as terms
        """ % (dp_uri)
        print("get_datapoint unscheduled  query", query)
        result = session.run(query)
        for record in result:
          results.append(record.data())

    # for record in results:
    #   record['values'] = list(set(record['values']))

    # print("datapoint results", results)

    db.close()
    if not results:
      return {'error': 'Failed to find BC attached to datapoint'}
    return results
    
  @staticmethod
  def _add_property(bc, code, decode):
    uuids = {'property': str(uuid4()), 'code': str(uuid4()), 'aliasCode': str(uuid4())}
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (bc:BiomedicalConcept {uuid: '%s'})
        WITH bc
        CREATE (c:Code {uuid: $s_uuid1, id: 'tbd', code: '%s', codeSystem: 'http://www.cdisc.org', codeSystemVersion: '2023-09-29', decode: '%s', instanceType: 'Code'})
        CREATE (ac:AliasCode {uuid: $s_uuid2, id: 'tbd', instanceType: 'AliasCode'})
        CREATE (p:BiomedicalConceptProperty {uuid: $s_uuid3, id: 'tbd', name: '--DTC', label: 'Date Time', isRequired: 'true', isEnabled: 'true', datatype: 'datetime', instanceType: 'BiomedicalConceptProperty'})
        CREATE (bc)-[:PROPERTIES_REL]->(p)-[:CODE_REL]->(ac)-[:STANDARD_CODE_REL]->(c)
        RETURN p.uuid as uuid
      """ % (bc.uuid, code, decode)
      result = session.run(query, 
        s_uuid1=str(uuid4()), 
        s_uuid2=str(uuid4()), 
        s_uuid3=str(uuid4())
      )
      for row in result:
        return uuids

  @staticmethod
  def _copy_surrogate(sd_uuid):
    db = Neo4jConnection()
    bc_uuid = str(uuid4())

    with db.session() as session:
      results = []
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[:BC_SURROGATES_REL]->(bcs:BiomedicalConceptSurrogate)
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
      """ % (sd_uuid, bc_uuid)
      # print("query", query)
      records = session.run(query)
      for record in records:
        results.append(record.data())
    db.close()
    if results:
      return results[0]['uuid']
    return None

  @staticmethod
  def _copy_properties(sd_uuid, bc_uuid, new_bc_name, copy_bc_name):
    db = Neo4jConnection()
    bcp_uuids = []
    with db.session() as session:
      # Get properties for bc to copy
      query = """
          MATCH (sd:StudyDesign {uuid: '%s'})-[:BIOMEDICAL_CONCEPTS_REL]->(bc:BiomedicalConcept {name:"%s"})-[:PROPERTIES_REL]->(bcp)
          RETURN bcp.uuid as uuid, bcp.name as name, bcp.label as label
      """ % (sd_uuid, copy_bc_name)
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
              SET bcp.datatype     =	'date'
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
    db.close()
    return bcp_uuids
      
  @staticmethod
  def _copy_bc_relationships_from_bc(sd_uuid, bc_uuid, copy_bc_name):
    db = Neo4jConnection()
    with db.session() as session:
      # Get uuid of BC to copy relationships
      query = """
          MATCH (sd:StudyDesign {uuid: '%s'})-[:BIOMEDICAL_CONCEPTS_REL]->(copy_bc:BiomedicalConcept {name:"%s"})
          return copy_bc.uuid as copy_bc_uuid
      """ % (sd_uuid, copy_bc_name)
      results = session.run(query)
      copy_bc_uuid = None
      for result in results.data():
        copy_bc_uuid = result['copy_bc_uuid']

      if copy_bc_uuid:
        # Copy relationship to study
        query = """
            MATCH (sd:StudyDesign {uuid: '%s'})
            MATCH (new_bc:BiomedicalConcept {uuid:"%s"})
            MERGE (new_bc)<-[:BIOMEDICAL_CONCEPTS_REL]-(sd)
        """ % (sd_uuid, bc_uuid)
        # print("sd to bc", query)
        results = session.run(query)
        # for result in results:
        #   print("result",result.data())
        print("Created link to Study Design")

        # Copy relationship to domain
        query = """
            MATCH (copy_bc:BiomedicalConcept {uuid:"%s"})<-[:USING_BC_REL]-(target:Domain)
            MATCH (new_bc:BiomedicalConcept {uuid:"%s"})
            MERGE (new_bc)<-[:USING_BC_REL]-(target)
        """ % (copy_bc_uuid, bc_uuid)
        # print("rel to domain", query)
        results = session.run(query)
        print("Created link to Domain")

        # Copy relationship to activity
        query = """
            MATCH (copy_bc:BiomedicalConcept {uuid:"%s"})<-[:BIOMEDICAL_CONCEPT_REL]-(target:Activity)
            MATCH (new_bc:BiomedicalConcept {uuid:"%s"})
            MERGE (new_bc)<-[:BIOMEDICAL_CONCEPT_REL]-(target)
        """ % (copy_bc_uuid, bc_uuid)
        # print("rel to activity", query)
        results = session.run(query)
        print("Created link to Activity")

        # Adding CODE_REL -> AliasCode
        query = f"""
            MATCH (bc:BiomedicalConcept {{uuid:"{bc_uuid}"}})
            with bc
            CREATE (ac:AliasCode {{uuid: '{str(uuid4())}', instanceType: 'AliasCode', id: 'AliasCode_BD'}})
            CREATE (c:Code {{code: 'S000001', codeSystem: 'http://www.example.org', codeSystemVersion: '0.1', decode: 'Date of Birth', id: 'Code_436', instanceType: 'Code', uuid: '{str(uuid4())}'}})
            CREATE (bc)-[:CODE_REL]->(ac)-[:STANDARD_CODE_REL]->(c)
            return count(*) as count
        """
        # print("code_rel", query)
        results = session.run(query)
      else:
        application_logger.info("Did not find BC to copy from:", copy_bc_name)
    db.close()


  @staticmethod
  def _add_missing_links_to_crm():
    db = Neo4jConnection()
    with db.session() as session:
      # If topic result (e.g. Date of Birth)
      # if bcp['name'] != copy_bc_name:
      # bcp_name = "Date of Birth"

      var_link_crm = {
        'BRTHDTC':'https://crm.d4k.dk/dataset/common/period/period_start/date_time/value'
       ,'RFICDTC':'https://crm.d4k.dk/dataset/common/period/period_start/date_time/value'
       ,'DSDECOD':'https://crm.d4k.dk/dataset/observation/observation_result/result/coding/code'
       ,'DSSTDTC':'https://crm.d4k.dk/dataset/common/period/period_start/date_time/value'
       ,'DSDTC'  :'https://crm.d4k.dk/dataset/common/date_time/date_time/value'
       ,'DSTERM' :'https://crm.d4k.dk/dataset/observation/observation_result/result/quantity/value'
       ,'VSPOS'  :'https://crm.d4k.dk/dataset/observation/position/coding/code'
       ,'VSLOC'  :'https://crm.d4k.dk/dataset/common/location/coding/code'
      #  ,'DMDTC'  :'https://crm.d4k.dk/dataset/common/date_time/date_time/value'
       ,'EXDOSFRQ': 'https://crm.d4k.dk/dataset/therapeutic_intervention/frequency/coding/code'
       ,'EXROUTE': 'https://crm.d4k.dk/dataset/therapeutic_intervention/route/coding/code'
       ,'EXTRT': 'https://crm.d4k.dk/dataset/therapeutic_intervention/description/coding/code'
       ,'EXDOSFRM': 'https://crm.d4k.dk/dataset/therapeutic_intervention/form/coding/code'
       ,'EXDOSE': 'https://crm.d4k.dk/dataset/therapeutic_intervention/single_dose/quantity/value'
       ,'EXDOSU': 'https://crm.d4k.dk/dataset/therapeutic_intervention/single_dose/quantity/unit'
       ,'EXSTDTC': 'https://crm.d4k.dk/dataset/common/period/period_start/date_time/value'
       ,'EXENDTC': 'https://crm.d4k.dk/dataset/common/period/period_end/date_time/value'
       ,'AESTDTC': 'https://crm.d4k.dk/dataset/common/period/period_start/date_time/value'
       ,'AEENDTC': 'https://crm.d4k.dk/dataset/common/period/period_end/date_time/value'
       ,'AERLDEV'  : 'https://crm.d4k.dk/dataset/adverse_event/causality/device'
       ,'AERELNST' : 'https://crm.d4k.dk/dataset/adverse_event/causality/non_study_treatment'
       ,'AEREL'    : 'https://crm.d4k.dk/dataset/adverse_event/causality/related'
       ,'AEACNDEV' : 'https://crm.d4k.dk/dataset/adverse_event/response/concomitant_treatment'
       ,'AEACNOTH' : 'https://crm.d4k.dk/dataset/adverse_event/response/other'
       ,'AEACN'    : 'https://crm.d4k.dk/dataset/adverse_event/response/study_treatment'
       ,'AESER'    : 'https://crm.d4k.dk/dataset/adverse_event/serious'
       ,'AESEV'    : 'https://crm.d4k.dk/dataset/adverse_event/severity'
       ,'AETERM'   : 'https://crm.d4k.dk/dataset/adverse_event/term'
       ,'AETOXGR'  : 'https://crm.d4k.dk/dataset/adverse_event/toxicity/grade'
       ,'AELOC'  :'https://crm.d4k.dk/dataset/common/location/coding/code'
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
          # print("query",query)
    db.close()

  # Names/Labels of BC properties inconsistent
  # NOTE: This is a workaround
  # Introducing alt_sdtm_name to accomodate
  @staticmethod
  def _fix_bc_name_label():
    bcp_alt_name = {
      'DSSTDTC': 'RFICDTC'
    }
    db = Neo4jConnection()
    with db.session() as session:
      for bcp,alt_sdtm_name in bcp_alt_name.items():
        query = """
          MATCH (bcp:BiomedicalConceptProperty {name:'%s'})
          SET bcp.alt_sdtm_name = '%s'
          return bcp.alt_label
        """ % (bcp,alt_sdtm_name)
        # print("alt_sdtm_name query",query)
        results = db.query(query)
        if results:
          application_logger.info(f"Added alt_sdtm_name to {alt_sdtm_name} to bc property {bcp}")
        else:
          application_logger.info(f"Info: Failed to add alt_sdtm_name to bc property {bcp} ({alt_sdtm_name})")
          # print("query",query)
    db.close()
    return

  # Add missing terminology
  # NOTE: This is a workaround. Sex get's response codes, but not race.
  @staticmethod
  def _add_missing_terminology():
    codes = [
      {'bcp_name': 'Race', 'code':'C41260', 'decode':	'ASIAN'},
      {'bcp_name': 'Race', 'code':'C16352', 'decode':	'BLACK OR AFRICAN AMERICAN'},
      {'bcp_name': 'Race', 'code':'C41219', 'decode':	'NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER'},
      {'bcp_name': 'Race', 'code':'C43234', 'decode':	'NOT REPORTED'},
      {'bcp_name': 'Race', 'code':'C17649', 'decode':	'OTHER'},
      {'bcp_name': 'Race', 'code':'C17998', 'decode':	'UNKNOWN'},
      {'bcp_name': 'Race', 'code':'C41261', 'decode':	'WHITE'},
    ]

    db = Neo4jConnection()
    with db.session() as session:
        for c in codes:
          query = """
            MATCH (bcp:BiomedicalConceptProperty {name:'%s'})
            MERGE (r:ResponseCode {id:'rcid_%s', instanceType:'ResponseCode', isEnabled: True, uuid: '%s'})
            MERGE (c:Code {code:'%s', codeSystem: 'http://www.cdisc.org', codeSystemVersion: '2023-12-15', decode:	'%s', id: 'cid_%s', instanceType: 'Code', uuid: '%s'})
            MERGE (bcp)-[:RESPONSE_CODES_REL]->(r)-[:CODE_REL]->(c)
            return "done" as done
          """ % (c['bcp_name'], c['code'], str(uuid4()), c['code'], c['decode'], c['code'], str(uuid4()))
          # print('query', query)
          response = session.run(query)
          result = [x.data() for x in response]
          if len(result) > 0 and 'done' in result[0]:
            application_logger.info(f"BCP {c['bcp_name']}: Added term {c['code']} - {c['decode']}")
          else:
            application_logger.info(f"Info: BCP {c['bcp_name']} failed to create term term {c['code']} -{c['decode']}")
    db.close()
    return

  # NOTE: This is a workaround
  # NOTE: Adding link to CRM for BRTHDTC. https://crm.d4k.dk/dataset/common/period/period_start/date_time/value (--STDTC)
  @staticmethod
  def _add_brthdtc_link_crm():
    db = Neo4jConnection()
    with db.session() as session:
      # Add property BRTHDTC IS_A_REL to CRM. I
      query = f"""
          MATCH (bcp:BiomedicalConceptProperty {{name:'BRTHDTC'}})
          MATCH (crm_add:CRMNode {{uri:'https://crm.d4k.dk/dataset/common/period/period_start/date_time/value'}})
          MERGE (bcp)-[:IS_A_REL]->(crm_add)
          return "done" as result
      """
      # print(query)
      results = session.run(query)
      application_logger.info("Linking BRTHDTC to Start date/time")
    db.close()
    return

  @staticmethod
  def _remove_properties_from_exposure(sd_uuid):
    db = Neo4jConnection()
    with db.session() as session:
      # NOTE: Just for simplifying life
      properties = ["EXREFID","EXLOC","EXFAST","EXDOSTXT","EXDOSRGM","EXDIR","EXLAT"]
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[:BIOMEDICAL_CONCEPTS_REL]->(bc:BiomedicalConcept {name:'Exposure Unblinded'})-[:PROPERTIES_REL]->(p:BiomedicalConceptProperty)
        WHERE  p.name in %s
        DETACH DELETE p
        RETURN count(p)
      """ % (sd_uuid, properties)
      # print("Delete Exposure Unblinded properties query",query)
      results = db.query(query)
      print("Delete exposure properties results",results)
      if results:
        application_logger.info(f"Removed {properties}")
      else:
        application_logger.info(f"Info: Failed to remove {properties}")

  @staticmethod
  def _get_bcs_by_name(sd_uuid, name):
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[:BIOMEDICAL_CONCEPTS_REL]->(bc:BiomedicalConcept) WHERE bc.name = '%s' RETURN DISTINCT bc
      """ % (sd_uuid, name)
      result = session.run(query)
      for record in result:
        results.append(BiomedicalConceptSimple.wrap(record['bc']))
      return results

  @staticmethod
  def _add_properties_to_ae(bcs):
    for bc in bcs:
      # print("bc",bc)
      properties = [
        {'name': 'AELLT','label': 'MedDRA Lowest Level Term','datatype': 'integer','isRequired': True, 'isEnabled': True},
        {'name': 'AEBODSYS','label': 'Body System','datatype': 'string','isRequired': True, 'isEnabled': True}
      ]
      # print("properties",properties)


      db = Neo4jConnection()
      with db.session() as session:
        uuids = {'property': str(uuid4()), 'code': str(uuid4()), 'aliasCode': str(uuid4())}
        for p in properties:
          query = """
            MATCH (bc:BiomedicalConcept {uuid: '%s'})
            WITH bc
            CREATE (c:Code {uuid: $s_uuid1, id: 'tbd', code: '%s', codeSystem: 'http://www.cdisc.org', codeSystemVersion: '2023-09-29', decode: '%s', instanceType: 'Code'})
            CREATE (ac:AliasCode {uuid: $s_uuid2, id: 'tbd', instanceType: 'AliasCode'})
            CREATE (p:BiomedicalConceptProperty {uuid: $s_uuid3, id: 'tbd', name: '%s', label: '%s', isRequired: %s, isEnabled: %s, datatype: '%s', instanceType: 'BiomedicalConceptProperty'})
            CREATE (bc)-[:PROPERTIES_REL]->(p)-[:CODE_REL]->(ac)-[:STANDARD_CODE_REL]->(c)
            RETURN p.uuid as uuid
          """ % (bc.uuid, p['name'], p['label'], p['name'], p['label'], p['isRequired'], p['isEnabled'], p['datatype'])
          # print("query",query)
          result = session.run(query, 
            s_uuid1=str(uuid4()), 
            s_uuid2=str(uuid4()), 
            s_uuid3=str(uuid4())
          )
          for row in result:
            p_uuid = [r['uuid'] for r in result]
          application_logger.info(f"Added AE property '{p['name']}' {p_uuid}")
    db.close()
