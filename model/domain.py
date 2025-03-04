import pandas as pd
from typing import List, Optional
from model.base_node import BaseNode
from model.variable import Variable
from model.biomedical_concept import BiomedicalConceptSimple
from d4kms_service import Neo4jConnection
from d4kms_generic import application_logger
from dateutil import parser 
from model.utility.configuration import ConfigurationNode, Configuration

class Domain(BaseNode):
  uri: str
  name: str
  label: str
  structure: str
  goc: Optional[str] = None
  ordinal: int
  items: List[Variable] = []
  bc_references: List[str] = []
  configuration: dict = {}
  sd_uuid: str = ""

  def bcs(self, page, size, filter):
    skip_offset_clause = ""
    if page != 0:
      offset = (page - 1) * size
      skip_offset_clause = "SKIP %s LIMIT %s" % (offset, size)
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (d:Domain {uuid: '%s'})-[:USING_BC_REL]->(bc:BiomedicalConcept)
        RETURN COUNT(DISTINCT(bc.name)) AS count
      """ % (self.uuid)
      #print(f"QUERY: {query}")
      result = session.run(query)
      count = 0
      for record in result:
        count = record['count']
      query = """
        MATCH (d:Domain {uuid: '%s'})-[:USING_BC_REL]->(bc:BiomedicalConcept)
        RETURN DISTINCT(bc.name) as name ORDER BY name %s
      """ % (self.uuid, skip_offset_clause)
      #print(f"QUERY: {query}")
      result = session.run(query)
      results = []
      for record in result:
        results.append(record['name'])
    result = {'items': results, 'page': page, 'size': size, 'filter': filter, 'count': count }
    return result

  def unlink(self, name):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (d:Domain {uuid: '%s'})-[r:USING_BC_REL]->(bc:BiomedicalConcept) WHERE bc.name='%s'
        DELETE r
        RETURN DISTINCT(bc.name) as name
      """ % (self.uuid, name)
      #print(f"QUERY: {query}")
      result = session.run(query)
      for record in result:
        return record['name']
    return None

  def link(self, name):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (d:Domain {uuid: '%s'}), (bc:BiomedicalConcept) WHERE bc.name='%s'
        MERGE (d)-[r:USING_BC_REL]->(bc)
        RETURN DISTINCT(bc.name) as name
      """ % (self.uuid, name)
      #print(f"QUERY: {query}")
      result = session.run(query)
      for record in result:
        return record['name']
    return None

  def data(self):
    self.configuration = ConfigurationNode.get()
    self.sd_uuid = self.get_study_uuid()
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      if self.name == "DM":
        query = self.dm_query()
      elif self.name == "DS":
        query = self.ds_query()
      elif self.name == "AE":
        query = self.ae_query()
      elif self.name == "EX":
        query = self.intervention_query()
      else:
        query = self.findings_query()
      # print("sdtm", query)
      rows = session.run(query)
      for row in rows:
        record = { 
          'variable': row["variable"], 
          'value': row["value"], 
          'DOMAIN': row["DOMAIN"], 
          'STUDYID': row["STUDYID"], 
          'USUBJID': row["SITEID"]+"-"+row["SUBJID"],
        }
        if 'uuid' in row.keys():
          record['uuid'] = row['uuid']
        if 'VISIT' in row.keys():
          record['VISIT'] = row["VISIT"]
        if 'VISITNUM' in row.keys():
          record['VISITNUM'] = row["VISITNUM"]
        if 'VISITDY' in row.keys():
          record['VISITDY'] = row["VISITDY"]
        if 'TPT' in row.keys():
          record['TPT'] = row["TPT"]
        if 'baseline_timing' in row.keys():
          record['baseline_timing'] = row["baseline_timing"]
        if 'EPOCH' in row.keys():
          record['EPOCH'] = row["EPOCH"]
        if 'term' in row.keys():
          record['term'] = row["term"]
        if '--DTC' in row.keys():
          record[self.name+'DTC'] = row["--DTC"]
        if 'dp_uri' in row.keys():
          record['dp_uri'] = row["dp_uri"]
        if self.name == "DM":
          record['SUBJID'] = row["SUBJID"]
          record['SITEID'] = row["SITEID"]
          if 'COUNTRY' in row.keys():
            record['COUNTRY'] = row["COUNTRY"] 
          if 'INVNAM' in row.keys():
            record['INVNAM'] = row["INVNAM"]
          if 'INVID' in row.keys():
            record['INVID'] = row["INVID"]
        elif self.name == "DS":
          for variable in ['EPOCH','DSDTC','decod']:
            if variable in row.keys():
              record[variable] = row[variable]
        elif self.name == "EX":
          record['trt'] = row['trt']
        elif self.name == "AE":
          record['key'] = row['key']
          # record['term'] = row['term']
        else:
          record['test_code'] = row['test_code']
          record['test_label'] = row['test_label']
        results.append(record)
      # for x in results[0:5]:
      #   print(x)
      if self.name == "DM":
        df = self.construct_dm_dataframe(results)
      elif self.name == "DS":
        # print("Creating DS dataframe")
        df = self.construct_ds_dataframe(results)
      elif self.name == "AE":
        # print("Creating AE dataframe")
        # for x in results:
        #   print(x)
        df = self.construct_ae_dataframe(results)
      elif self.name == "EX":
        # print("Creating EX dataframe")
        df = self.construct_intervention_dataframe(results)
      else:
        df = self.construct_findings_dataframe(results)
      result = df.to_dict('index')
      # result = df.to_dict('records')
      return result

  def dm_query(self):
    # FIX: REMOVE HARD CODING OF ELI LILLY
    # Removed "or bcp.name = crm.sdtm" from query. Only to get DMDTC which is not needed
    query = """
      MATCH (sd:StudyDesign)-[:DOMAIN_REL]->(domain:Domain {uuid:'%s'})
      MATCH (sd)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)
      MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(sis:Organization {name:'Eli Lilly'})
      WITH si, domain
      MATCH (domain)-[:USING_BC_REL]-(bc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
      MATCH (bcp)<-[:PROPERTIES_REL]-(dc:DataContract)
      MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)
      MATCH (dc)<-[:FOR_DC_REL]-(dp:DataPoint)
      MATCH (dp)-[:FOR_SUBJECT_REL]->(subj:Subject)
      MATCH (subj)-[:ENROLLED_AT_SITE_REL]->(site:StudySite)
      OPTIONAL MATCH (site)-[:LEGAL_ADDRESS_REL]->(:Address)-[:COUNTRY_REL]->(country1:Code)
      OPTIONAL MATCH (site)<-[:MANAGES_REL]-(:ResearchOrganization)-[:LEGAL_ADDRESS_REL]->(:Address)-[:COUNTRY_REL]->(country2:Code)
      MATCH (domain)-[:VARIABLE_REL]->(var:Variable)-[:IS_A_REL]->(crm)
      MATCH (dc)-[:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(tim:Timing)
      MATCH (act_inst_main)-[:ENCOUNTER_REL]->(e:Encounter)
      MATCH (act_inst_main)-[:EPOCH_REL]->(epoch:StudyEpoch)
      WHERE  var.label = bcp.label or bcp.alt_sdtm_name = var.name
      return
      si.studyIdentifier as STUDYID
      , domain.name as DOMAIN
      , toInteger(subj.identifier) as n_subject
      , case toInteger(subj.identifier) when subj.identifier is null then 0 else 1 end as n_order
      , subj.identifier as SUBJID
      , var.name as variable
      , dp.value as value
      , dp.uri as dp_uri
      , site.name as SITEID
      , e.label as VISIT
      , epoch.label as EPOCH
      , coalesce(country1.code, country2.code) as COUNTRY
      order by SITEID, n_order, SUBJID
    """ % (self.uuid)
    # print(query)
    return query

  def ds_query(self):
    # ISSUE: Term is not the value from CRF. It is the standard decode of the BC
    # FIX: REMOVE HARD CODING OF ELI LILLY
    query = """
      MATCH (sd:StudyDesign)-[:DOMAIN_REL]->(domain:Domain {uuid:'%s'})
      MATCH (sd)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)
      MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(sis:Organization {name:'Eli Lilly'})
      WITH si, domain
      MATCH (domain)-[:USING_BC_REL]-(bc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
      OPTIONAL MATCH (bc)-[:CODE_REL]->(:AliasCode)-[:STANDARD_CODE_REL]->(c:Code)
      MATCH (bcp)<-[:PROPERTIES_REL]-(dc:DataContract)
      MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)
      MATCH (dc)<-[:FOR_DC_REL]-(dp:DataPoint)
      MATCH (dp)-[:FOR_SUBJECT_REL]->(subj:Subject)
      MATCH (subj)-[:ENROLLED_AT_SITE_REL]->(site:StudySite)
      MATCH (domain)-[:VARIABLE_REL]->(var:Variable)-[:IS_A_REL]->(crm)
      MATCH (dc)-[:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(tim:Timing)
      MATCH (act_inst_main)-[:ENCOUNTER_REL]->(e:Encounter)
      MATCH (act_inst_main)-[:EPOCH_REL]->(epoch:StudyEpoch)
      // WHERE  var.label = bcp.label
      WHERE  var.name = bcp.name
      return
            si.studyIdentifier as STUDYID
            , domain.name as DOMAIN
            , subj.identifier as SUBJID
            , c.decode as term
            , bc.name as decod
            , var.name as variable
            , dp.value as value
            , site.name as SITEID
            , e.label as VISIT
            , epoch.label as EPOCH
            , bc.uuid as bc_uuid
    """ % (self.uuid)
    # print("ds_query",query)
    return query

  def ae_query(self):
    # FIX: REMOVE HARD CODING OF ELI LILLY
    query = """
      MATCH (sd:StudyDesign)-[:DOMAIN_REL]->(domain:Domain {uuid:'%s'})
      MATCH (sd)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)
      MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(sis:Organization {name:'Eli Lilly'})
      WITH si, domain
      MATCH (domain)-[:USING_BC_REL]-(bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
      MATCH (bcp)<-[:PROPERTIES_REL]->(dc:DataContract)
      MATCH (dc)<-[:FOR_DC_REL]-(:DataPoint)-[:SOURCE]->(r:Record)
      WITH si, domain, dc, r
      MATCH (r)<-[:SOURCE]-(dp:DataPoint)-[:FOR_SUBJECT_REL]->(subj:Subject)-[:ENROLLED_AT_SITE_REL]->(site:StudySite)
      MATCH (dp)-[:FOR_DC_REL]->(dc:DataContract)
      MATCH (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
      return
      si.studyIdentifier as STUDYID
      , domain.name as DOMAIN
      , subj.identifier as SUBJID
      , r.key as key
      , CASE bcp.name WHEN '--DTC' THEN 'AEDTC' ELSE bcp.name END as variable
      , dp.value as value
      , site.name as SITEID
      , bc.uuid as bc_uuid
      order by key
    """ % (self.uuid)
    # print("ae query",query)
    return query

  def intervention_query(self):
    # ISSUE: Now only EX. Need to check other interventions
    query = """
      MATCH (sd:StudyDesign)-[:DOMAIN_REL]->(domain:Domain {uuid:'%s'})
      MATCH (sd)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)
      MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(sis:Organization {name:'Eli Lilly'})
      WITH si, domain
      MATCH (domain)-[:USING_BC_REL]-(bc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
      OPTIONAL MATCH (bc)-[:CODE_REL]->(:AliasCode)-[:STANDARD_CODE_REL]->(c:Code)
      MATCH (bcp)<-[:PROPERTIES_REL]-(dc:DataContract)
      MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)
      MATCH (dc)<-[:FOR_DC_REL]-(dp:DataPoint)
      MATCH (dp)-[:FOR_SUBJECT_REL]->(subj:Subject)
      MATCH (subj)-[:ENROLLED_AT_SITE_REL]->(site:StudySite)
      MATCH (domain)-[:VARIABLE_REL]->(var:Variable)
      MATCH (dc)-[:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(tim:Timing)
      MATCH (act_inst_main)-[:ENCOUNTER_REL]->(e:Encounter)
      MATCH (act_inst_main)-[:EPOCH_REL]->(epoch:StudyEpoch)
      WHERE  var.name = bcp.name
      WITH si, domain, subj, c, bc, var, dp, site, e, epoch, collect(toInteger(split(e.id,'_')[1]))[0] as e_order
      return
      si.studyIdentifier as STUDYID
      , domain.name as DOMAIN
      , subj.identifier as SUBJID
      , c.decode as trt
      , bc.name as decod
      , var.name as variable
      , dp.value as value
      , dp.uri as dp_uri
      , site.name as SITEID
      , e.label as VISIT
      , e_order as VISITNUM
      , epoch.label as EPOCH
      , bc.uuid as bc_uuid
      order by SITEID, SUBJID, VISIT
    """ % (self.uuid)
    # print("intervention query",query)
    return query

  def findings_query(self):
    # Query with timing.value as VISITDY
    query = """
      match(domain:Domain{uuid:'%s'})-[:VARIABLE_REL]->(var:Variable)-[:IS_A_REL]->(crm:CRMNode)<-[:IS_A_REL]-(bc_prop:BiomedicalConceptProperty), (domain)-[:USING_BC_REL]->(bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bc_prop)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(tim:Timing)-[:TYPE_REL]->(tim_ref:Code)
      OPTIONAL MATCH(act_inst_main)-[:ENCOUNTER_REL]->(e:Encounter),(act_inst_main)-[:EPOCH_REL]->(epoch:StudyEpoch)
      OPTIONAL MATCH(act_inst_main)<-[:INSTANCES_REL]-(tl:ScheduleTimeline)
      MATCH (domain)<-[:DOMAIN_REL]-(sd:StudyDesign)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(sis:Organization {name:'Eli Lilly'})
      MATCH (dc)<-[:FOR_DC_REL]-(d:DataPoint)-[:FOR_SUBJECT_REL]->(s:Subject)-[:ENROLLED_AT_SITE_REL]->(site:StudySite)
      MATCH (bc)-[:CODE_REL]->()-[:STANDARD_CODE_REL]-(code:Code)
      WITH code.decode as test_code, si,domain, collect(epoch.name) as epoch,collect(toInteger(split(e.id,'_')[1])) as e_order,var, bc, dc, d, s, site, collect(e.label) as vis, apoc.map.fromPairs(collect([tl.label,tim.value])) as TP, tim_ref.decode as tim_ref
      WITH test_code, si,domain, epoch,e_order[0] as e_order,var, bc, dc, d, s, site, apoc.text.join(apoc.coll.remove(keys(TP),apoc.coll.indexOf(keys(TP),'Main Timeline')),',') as timelines, TP, apoc.text.join(vis,',') as visit, tim_ref
      RETURN 
      si.studyIdentifier as STUDYID
      ,domain.name as DOMAIN
      ,s.identifier as SUBJID
      ,test_code as test_code
      ,bc.name as test_label
      ,var.name as variable
      ,d.value as value
      ,d.uri as dp_uri
      ,e_order as VISITNUM
      ,site.name as SITEID
      ,visit as VISIT
      ,TP['Main Timeline'] as VISITDY
      ,tim_ref as baseline_timing
      ,duration(TP['Main Timeline']) as ord
      ,TP[timelines] as TPT
      ,epoch[0] as  EPOCH 
      ,bc.uri as uuid
      order by SITEID, SUBJID, VISITNUM, ord ,VISIT, test_code
    """ % (self.uuid)
    return query

  def convert_str_datetime(self, date_time_str):
    # ASSUMPTION: Assuming complete dates are sent
    # ISSUE: parser.parse is said to be able to handle incomplete dates, but might need fixing
    return parser.parse(date_time_str)

  def sdtm_derive_age(self,reference_start_date,brthdtc):
    if reference_start_date and brthdtc:
      inclusion_date = self.convert_str_datetime(reference_start_date)
      birth_date = self.convert_str_datetime(brthdtc)
      # Note. Formula is using the fact that Python can subtract boolean from integer. (True = 1 and False = 0)
      age = inclusion_date.year - birth_date.year - ((inclusion_date.month, inclusion_date.day) < (birth_date.month, birth_date.day))
    else:
      application_logger.info(f"Could not derive age. reference_start_date:{reference_start_date} brthdtc:{brthdtc}")
      age = "N/A"
    return age

  def sdtm_derive_dy(self, start_date_str, end_date_str):
    try:
      if start_date_str and end_date_str:
        ref_start = self.convert_str_datetime(start_date_str)
        dtc = self.convert_str_datetime(end_date_str)
        dy = dtc - ref_start
        # print("ref_start",ref_start,"dtc",dtc,"dy",dy)
        if ref_start > dtc:
          return dy.days
        else:
          return dy.days + 1
      return
    except Exception as e:
      application_logger.exception(f"Could not derive duration/dy: {start_date_str} {end_date_str}", e)
      return False

  def add_seq_dict(self, results, seq_index, usubjid_index):
    assert seq_index, "add_seq_dict: parameter 'seq_index' missing"
    assert usubjid_index, "add_seq_dict: parameter 'usubjid_index' missing"
    # print("adding seq dict",seq_index,usubjid_index)
    current_usubjid = ""
    seq = 0
    for key, result in results.items():
      if current_usubjid == result[usubjid_index]:
        seq += 1
        result[seq_index] = seq
      else:
        seq = 1
        result[seq_index] = seq
        current_usubjid = result[usubjid_index]
    # print("Sorting dict")
    # results = sorted(results, key=lambda key: key)

  def add_seq_list_of_dict(self, results, seq_var):
    # print("adding seq list of dict", seq_var)
    current_usubjid = ""
    seq = 0
    for result in results:
      if current_usubjid == result['USUBJID']:
        seq += 1
        result[seq_var] = seq
      else:
        seq = 1
        result[seq_var] = seq
        current_usubjid = result['USUBJID']
    # print("Sorting list of dict")
    # results = sorted(results, key=lambda row: (row['USUBJID'],row[seq_var]))

  def add_seq(self, results, seq_index = None, usubjid_index = None):
    seq_var = self.name + "SEQ"
    if isinstance(results, dict):
      self.add_seq_dict(results, seq_index, usubjid_index)
    elif isinstance(results, list) and isinstance(results[0], dict):
      self.add_seq_list_of_dict(results, seq_var)
    else:
        application_logger.info(f"add_seq: Don't know how to add --SEQ to {results.__class__}")

  def add_duration_dict(self, results, index_dur, index_stdtc, index_endtc):
    for key, result in results.items():
      result[index_dur] = self.sdtm_derive_dy(result[index_stdtc], result[index_endtc])

  # def add_duration_list_of_dict(self, results, ):
  #   for key, result in results.items():
  #     result[index_dur] = self.sdtm_derive_dy(result[index_stdtc], result[index_endtc])

  def get_study_uuid(self):
    db = Neo4jConnection()
    with db.session() as session:
      query = """MATCH (sd:StudyDesign)-[:DOMAIN_REL]->(domain:Domain {uuid:'%s'}) return sd.uuid as uuid""" % (self.uuid)
      results = session.run(query)
      for result in results:
        return result['uuid']
      return None

  def get_reference_start_dates(self):
    db = Neo4jConnection()
    with db.session() as session:
      # FIXED: Query is unique for study
      # FIX: MAKE QUERY GENERIC USING CONFIGURATION
      # FIX: SHOULD USE REFERENCE START DATE, NOT INFORMED CONSENT
      query = """
        MATCH (site:StudySite)<-[:ENROLLED_AT_SITE_REL]-(s:Subject)<-[:FOR_SUBJECT_REL]-(dp:DataPoint)-[:FOR_DC_REL]->(dc:DataContract)-[:INSTANCES_REL]->(sai:ScheduledActivityInstance)-[:ENCOUNTER_REL]->(e:Encounter)
        MATCH (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)<-[:BIOMEDICAL_CONCEPTS_REL]-(:StudyDesign {uuid: '%s'})
        WHERE e.label = "Baseline"
        and bcp.name = "--DTC"
        return distinct site.name + '-' + s.identifier as USUBJID, dp.value as reference_date
      """ % (self.sd_uuid)
      # print("reference start date query",query)
      results = session.run(query)
      return [result.data() for result in results]

  def get_exposure_max_min_dates(self):
    bcs = self.configuration.study_product_bcs
    crm_start = self.configuration.crm_start
    crm_end = self.configuration.crm_end
    data = []
    db = Neo4jConnection()
    with db.session() as session:
      # FIXED: Query is unique for study
      # FIXED: Query uses configuration for exposure BCs and start/end from crm
      # NOTE: Fix just because SDTM makes things complicated: Splitting up in two questions so that I always get the start and end datapoint (if EXSTDTC = EXENDTC it might be any of them)
      # query = """
      #   MATCH (site:StudySite)<-[:ENROLLED_AT_SITE_REL]-(s:Subject)<-[:FOR_SUBJECT_REL]-(dp:DataPoint)-[:FOR_DC_REL]->(dc:DataContract)-[:INSTANCES_REL]->(sai:ScheduledActivityInstance)-[:ENCOUNTER_REL]->(e:Encounter)
      #   MATCH (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)<-[:BIOMEDICAL_CONCEPTS_REL]-(:StudyDesign {uuid: '%s'})
      #   MATCH (bcp)-[:IS_A_REL]-(crm:CRMNode)
      #   WHERE bc.name in %s and crm.sdtm in ['%s','%s']
      #   WITH site.name + '-' + s.identifier as identifier, apoc.agg.minItems(dp, dp.value) as minData, apoc.agg.maxItems(dp, dp.value) as maxData
      #   WITH identifier, {value: minData.value, uri: minData.items[0].uri} as min, {value: maxData.value, uri: maxData.items[0].uri} as max
      #   return identifier, min, max
      # """ % (self.sd_uuid, bcs, crm_start, crm_end)
      # print("ex min", query)
      # results = session.run(query)
      query = """
        MATCH (site:StudySite)<-[:ENROLLED_AT_SITE_REL]-(s:Subject)<-[:FOR_SUBJECT_REL]-(dp:DataPoint)-[:FOR_DC_REL]->(dc:DataContract)-[:INSTANCES_REL]->(sai:ScheduledActivityInstance)-[:ENCOUNTER_REL]->(e:Encounter)
        MATCH (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)<-[:BIOMEDICAL_CONCEPTS_REL]-(:StudyDesign {uuid: '%s'})
        MATCH (bcp)-[:IS_A_REL]-(crm:CRMNode)
        WHERE bc.name in %s and crm.sdtm = '%s'
        WITH site.name + '-' + s.identifier as identifier, apoc.agg.minItems(dp, dp.value) as minData
        WITH identifier, {value: minData.value, uri: minData.items[0].uri} as min
        return identifier, min
      """ % (self.sd_uuid, bcs, crm_start)
      # print("ex min", query)
      results = session.run(query)
      min_data = [result.data() for result in results]
      query = """
        MATCH (site:StudySite)<-[:ENROLLED_AT_SITE_REL]-(s:Subject)<-[:FOR_SUBJECT_REL]-(dp:DataPoint)-[:FOR_DC_REL]->(dc:DataContract)-[:INSTANCES_REL]->(sai:ScheduledActivityInstance)-[:ENCOUNTER_REL]->(e:Encounter)
        MATCH (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)<-[:BIOMEDICAL_CONCEPTS_REL]-(:StudyDesign {uuid: '%s'})
        MATCH (bcp)-[:IS_A_REL]-(crm:CRMNode)
        WHERE bc.name in %s and crm.sdtm = '%s'
        WITH site.name + '-' + s.identifier as identifier, apoc.agg.minItems(dp, dp.value) as minData, apoc.agg.maxItems(dp, dp.value) as maxData
        WITH identifier, {value: maxData.value, uri: maxData.items[0].uri} as max
        return identifier, max
      """ % (self.sd_uuid, bcs, crm_end)
      # print("ex max", query)
      results = session.run(query)
      max_data = [result.data() for result in results]
      for item in min_data:
        min_max = {}
        min_max['identifier'] = item['identifier']
        min_max['min'] = item['min']
        max = next((x['max'] for x in max_data if x['identifier'] == item['identifier']), None)
        min_max['max'] = max
        data.append(min_max)
    db.close()
    return data

  @staticmethod
  def create_uri_var(variable):
    return 'dp_uri_'+variable

  def construct_dm_dataframe(self, results):
    multiples = {}
    multiples_uri = {}
    supp_quals = {}
    column_names = self.variable_list()
    variables_with_uri = list(set([x['variable'] for x in results]))
    for x in variables_with_uri:
      column_names.append(self.create_uri_var(x))
    # Get reference dates
    reference_dates = self.get_reference_start_dates()
    dose_dates = self.get_exposure_max_min_dates()
    column_names.append('dp_uri_RFXSTDTC')
    column_names.append('dp_uri_RFXENDTC')

    # Check if age can be derived
    if all(key in column_names for key in ('RFICDTC','BRTHDTC')):
      derive_age = True
    # print("COLS:", column_names)
    final_results = {}
    for result in results:
      key = result["SUBJID"]
      if not result["SUBJID"] in final_results:  # Subject does not exist yet in result. Create default variables
        multiples[key] = {}
        multiples_uri[key] = {}
        final_results[key] = [""] * len(column_names)
        final_results[key][column_names.index("STUDYID")] = result["STUDYID"]
        final_results[key][column_names.index("DOMAIN")] = result["DOMAIN"]
        # final_results[key][column_names.index("USUBJID")] = "%s.%s" % (result["STUDYID"], result["SUBJID"])
        final_results[key][column_names.index("USUBJID")] = result["USUBJID"]
        final_results[key][column_names.index("SUBJID")] = result["SUBJID"]
        final_results[key][column_names.index("SITEID")] = result["SITEID"]
        if "INVID" in result.keys():
          final_results[key][column_names.index("INVID")] = result["INVID"]
        if "INVNAM" in result.keys():
          final_results[key][column_names.index("INVNAM")] = result["INVNAM"]
        if "COUNTRY" in result.keys():
          final_results[key][column_names.index("COUNTRY")] = result["COUNTRY"]
        if "RFXSTDTC" in column_names and "RFXENDTC" in column_names :
          dose_start_end = next((item for item in dose_dates if item["identifier"] == result['USUBJID']), None)
          if dose_start_end:
            final_results[key][column_names.index("RFXSTDTC")] = dose_start_end['min']['value']
            final_results[key][column_names.index("dp_uri_RFXSTDTC")] = dose_start_end['min']['uri']
            final_results[key][column_names.index("RFXENDTC")] = dose_start_end['max']['value']
            final_results[key][column_names.index("dp_uri_RFXENDTC")] = dose_start_end['max']['uri']


      variable_name = result["variable"]
      variable_index = [column_names.index(variable_name)][0]
      if 'dp_uri' in result:
        uri_var = self.create_uri_var(result['variable'])
        uri_index = [column_names.index(uri_var)][0]

      if not final_results[key][variable_index] == "": # Subject already have a value for variable
        if result["value"] != final_results[key][variable_index]:
          if not variable_name in multiples[key]: # create "multiple"
            multiples[key][variable_name] = [final_results[key][variable_index]]
            multiples_uri[key][variable_name] = [final_results[key][uri_index]]
            final_results[key][variable_index] = "MULTIPLE"
            final_results[key][uri_index] = "removed"
            if not variable_name in supp_quals:
              supp_quals[variable_name] = 1
          multiples[key][variable_name].append(result["value"])
          multiples_uri[key][variable_name].append(result["dp_uri"])
          if len(multiples[key][variable_name]) > supp_quals[variable_name]:
            supp_quals[variable_name] = len(multiples[key][variable_name])
      else: # Subject does not have a value for variable
        final_results[key][variable_index] = result["value"]
        final_results[key][column_names.index(uri_var)] = result['dp_uri']

      # Derive --DY
      if result["variable"] == self.name+"DTC":
        ref_date = next((item for item in reference_dates if item["USUBJID"] == result['USUBJID']), None)
        if ref_date:
          if self.name+'DY' in column_names:
            index_dy = [column_names.index(self.name+'DY')][0]
            final_results[key][index_dy] = self.sdtm_derive_dy(ref_date['reference_date'],result['value'])

    for supp_name, count in supp_quals.items():
      # print("Count: ", count)
      for i in range(1, count + 1):
        name = "%s%s" % (supp_name, i)
        column_names.append(name)
        uri_var = self.create_uri_var(name)
        column_names.append(uri_var)
        # print("Index: ", column_names.index(name))
        for subject, items in multiples.items():
          final_results[subject].append("") # Add original supp
          final_results[subject].append("") # Add dp supp
          if supp_name in items:
            # print("I: ", i)
            # print("Items: ", items[supp_name])
            if i <= len(items[supp_name]):
              # print("c", items)
              final_results[subject][column_names.index(name)] = items[supp_name][i - 1]
              final_results[subject][column_names.index(uri_var)] = multiples_uri[subject][supp_name][i - 1]
              # print("adding", multiples_uri[subject][supp_name][i - 1])
              #print("[%s] %s -> %s" % (subject, name, items[supp_name][i - 1]))

    if derive_age:
      index_rficdtc = [column_names.index('RFICDTC')][0]
      index_brthdtc = [column_names.index('BRTHDTC')][0]
      index_age = [column_names.index('AGE')][0]
      for key,vars in final_results.items():
        vars[index_age] = self.sdtm_derive_age(vars[index_rficdtc],vars[index_brthdtc])
        # vars[index_age] = derive_age(vars[index_rficdtc],vars[index_brthdtc])

    df = pd.DataFrame(columns=column_names)
    # print(df.head())
    for subject, result in final_results.items():
      df.loc[len(df.index)] = result
    # print(df.head())
    return df

  def first_last_exposure_of_study_drug(self):
    bcs = self.configuration.study_product_bcs
    crm_start = self.configuration.crm_start
    crm_end = self.configuration.crm_end
    db = Neo4jConnection()
    # query = self.intervention_query()
    with db.session() as session:
      # FIXED: Query is unique for study
      # FIXED: Query is generic. Query uses configuration for exposure BCs and start/end from crm
      query = """
        MATCH (site:StudySite)<-[:ENROLLED_AT_SITE_REL]-(subj:Subject)<-[:FOR_SUBJECT_REL]-(dp:DataPoint)-[:FOR_DC_REL]->(dc:DataContract)-[:INSTANCES_REL]->(sai:ScheduledActivityInstance)-[:ENCOUNTER_REL]->(e:Encounter)
        MATCH (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)<-[:BIOMEDICAL_CONCEPTS_REL]-(sd:StudyDesign {uuid: '%s'})
        MATCH (bcp)-[:IS_A_REL]-(crm:CRMNode)
        WHERE bc.name in %s and crm.sdtm in ['%s']
        WITH *
        MATCH (sd:StudyDesign)-[:BIOMEDICAL_CONCEPTS_REL]->(bc)
        MATCH (sd)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)
        MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(sis:Organization {name:'Eli Lilly'})
        // MATCH (dc)-[:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(tim:Timing)
        MATCH (dc)-[:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)
        MATCH (act_inst_main)-[:ENCOUNTER_REL]->(e:Encounter)
        MATCH (act_inst_main)-[:EPOCH_REL]->(epoch:StudyEpoch)
        with si, subj, crm, bc, dp, epoch, site
        RETURN
        si.studyIdentifier as STUDYID
        , "DS" as DOMAIN
        , site.name + '-' + subj.identifier as USUBJID
        , crm.sdtm as crm
        , bc.name as bc
        , "First Exposure" as term
        , "First Exposure to Study Drug" as decod
        , "DSSTDTC" as variable
        , dp.value as value
        , epoch.label as EPOCH
        order by USUBJID, value
      """ % (self.sd_uuid, bcs, crm_start)
      # """ % (self.sd_uuid, bcs, crm_start, crm_end)
      # print("first last exposure query", query)
      results = session.run(query)
      rows = []
      current_subject = ""
      for row in [result.data() for result in results]:
        if not row['USUBJID'] == current_subject:
          rows.append(row)
          current_subject = row['USUBJID']
      return rows


  def construct_ds_dataframe(self, results):
    multiples = {}
    supp_quals = {}
    seq_var = self.name+"SEQ"
    column_names = self.variable_list()
    column_names.remove("DSDTC")
    column_names.remove("DSDY")
    final_results = {}
    reference_dates = self.get_reference_start_dates()
    if 'first_exposure' in self.configuration.disposition:
      first_exposure_to_study_drug = self.first_last_exposure_of_study_drug()
      # results = results + first_exposure_to_study_drug
      results = sorted(results + first_exposure_to_study_drug, key=lambda i: (i['USUBJID'],i['value']))
    # if 'last_exposure' in self.configuration.disposition:
    #   first_exposure_to_study_drug = self.first_last_exposure_of_study_drug()
    for result in results:
      # print("DS",result)
      if 'DSSEQ' in result.keys():
        # key = "%s.%s" % (result['USUBJID'],result['DSSEQ'])
        key = "%s.%s.%s" % (result['USUBJID'],result['term'],result['DSSEQ'])
      else:
        # key = "%s." % (result['USUBJID'])
        key = "%s.%s" % (result['USUBJID'],result['term'])
      if not key in final_results:
        multiples[key] = {}
        final_results[key] = [""] * len(column_names)
        final_results[key][column_names.index("STUDYID")] = result["STUDYID"]
        final_results[key][column_names.index("DOMAIN")] = result["DOMAIN"]
        # final_results[key][column_names.index("USUBJID")] = "%s.%s" % (result["STUDYID"], result["SUBJID"])
        final_results[key][column_names.index("USUBJID")] = result["USUBJID"]
        if "DSSEQ" in result.keys():
          final_results[key][column_names.index("DSSEQ")] = result["DSSEQ"]
        # if "DSTERM" in result.keys():
        #   final_results[key][column_names.index("DSTERM")] = result["DSTERM"]
        if "term" in result.keys():
          final_results[key][column_names.index("DSTERM")] = result["term"]
        if "decod" in result.keys():
          final_results[key][column_names.index("DSDECOD")] = result["decod"]
          if result["decod"] in self.configuration.mandatory_bcs:
            final_results[key][column_names.index("DSCAT")] = "Protocol Milestone"
          else:
            final_results[key][column_names.index("DSCAT")] = "Other event"
        if "DSCAT" in result.keys():
          final_results[key][column_names.index("DSCAT")] = result["DSCAT"]
        if "DSSCAT" in result.keys():
          final_results[key][column_names.index("DSSCAT")] = result["DSSCAT"]
        if "EPOCH" in result.keys():
          final_results[key][column_names.index("EPOCH")] = result["EPOCH"]
      variable_index = column_names.index(result["variable"])
      variable_name = result["variable"]
      if not final_results[key][variable_index] == "":
        if result["value"] != final_results[key][variable_index]:
          if not variable_name in multiples[key]:
            multiples[key][variable_name] = [final_results[key][variable_index]]
            final_results[key][variable_index] = "MULTIPLE"
            if not variable_name in supp_quals:
              supp_quals[variable_name] = 1
          multiples[key][variable_name].append(result["value"])
          if len(multiples[key][variable_name]) > supp_quals[variable_name]:
            supp_quals[variable_name] = len(multiples[key][variable_name])
      else:
        final_results[key][variable_index] = result["value"]
      #print("[%s] %s -> %s, multiples %s" % (key, result["variable"], final_results[key][variable_index], multiples[key]))

    # Derive DSSTDY
    for key, result in final_results.items():
      usubjid_index = column_names.index('USUBJID')
      stdtc_index = column_names.index(self.name+'STDTC')
      stdy_index = column_names.index(self.name+'STDY')
      if stdtc_index and stdy_index:
        ref_date = next((item for item in reference_dates if item["USUBJID"] == result[usubjid_index]), None)
        if ref_date:
          result[stdy_index] = self.sdtm_derive_dy(ref_date['reference_date'],result[stdtc_index])

    for supp_name, count in supp_quals.items():
      #print("Count: ", count)
      for i in range(1, count + 1):
        name = "%s%s" % (supp_name, i)
        column_names.append(name)
        #print("Index: ", column_names.index(name))
        for subject, items in multiples.items():
          final_results[subject].append("")
          if supp_name in items:
            #print("I: ", i)
            #print("Items: ", items[supp_name])
            if i <= len(items[supp_name]):
              final_results[subject][column_names.index(name)] = items[supp_name][i - 1]
              #print("[%s] %s -> %s" % (subject, name, items[supp_name][i - 1]))

    self.add_seq(final_results,column_names.index(seq_var), column_names.index("USUBJID"))
    df = pd.DataFrame(columns=column_names)
    # print(df.head())
    for subject, result in final_results.items():
      df.loc[len(df.index)] = result
    # print(df.head())
    return df


  def construct_ae_dataframe(self, results):
    multiples = {}
    supp_quals = {}
    column_names = self.variable_list()
    pilot_study_vars = ['STUDYID', 'DOMAIN', 'USUBJID', 'AESEQ', 'AESPID', 'AETERM', 'AELLT', 'AELLTCD', 'AEDECOD', 'AEPTCD', 'AEHLT', 'AEHLTCD', 'AEHLGT', 'AEHLGTCD', 'AEBODSYS', 'AEBDSYCD', 'AESOC', 'AESOCCD', 'AESEV', 'AESER', 'AEACN', 'AEREL', 'AEOUT', 'AESCAN', 'AESCONG', 'AESDISAB', 'AESDTH', 'AESHOSP', 'AESLIFE', 'AESOD', 'AEDTC', 'AESTDTC', 'AEENDTC', 'AESTDY', 'AEENDY']
    column_names = [i for i in column_names if i in pilot_study_vars]
    column_names.insert(column_names.index('AESTDTC'),'AEDTC')
    final_results = {}
    reference_dates = self.get_reference_start_dates()
    for result in results:
      key = result['key']
      # if 'AESEQ' in result.keys():
      #   key = "%s.%s" % (result['USUBJID'],result['AESEQ'])
      # else:
      #   key = "%s." % (result['USUBJID'])
      if not key in final_results:
        multiples[key] = {}
        final_results[key] = [""] * len(column_names)
        final_results[key][column_names.index("STUDYID")] = result["STUDYID"]
        final_results[key][column_names.index("DOMAIN")] = result["DOMAIN"]
        # final_results[key][column_names.index("USUBJID")] = "%s.%s" % (result["STUDYID"], result["SUBJID"])
        final_results[key][column_names.index("USUBJID")] = result["USUBJID"]
        if "AESEQ" in result.keys():
          final_results[key][column_names.index("AESEQ")] = result["AESEQ"]
        if "AETERM" in result.keys():
          final_results[key][column_names.index("AETERM")] = result["AETERM"]
        if "term" in result.keys():
          final_results[key][column_names.index("AETERM")] = result["term"]
        if "decod" in result.keys():
          final_results[key][column_names.index("AEDECOD")] = result["decod"]
          if result["decod"] in self.configuration.mandatory_bcs:
            final_results[key][column_names.index("AECAT")] = "Protocol Milestone"
          else:
            final_results[key][column_names.index("AECAT")] = "Other event"

        if "AECAT" in result.keys():
          final_results[key][column_names.index("AECAT")] = result["AECAT"]
        # final_results[key][column_names.index("AESCAT")] = result["AESCAT"]
        if "EPOCH" in result.keys():
          final_results[key][column_names.index("EPOCH")] = result["EPOCH"]
        if "AEDTC" in result.keys():
          final_results[key][column_names.index("AEDTC")] = result["AEDTC"]
        if "AESTDTC" in result.keys():
          final_results[key][column_names.index("AESTDTC")] = result["AESTDTC"]
        if "AEENDTC" in result.keys():
          final_results[key][column_names.index("AEENDTC")] = result["AEENDTC"]
      variable_index = column_names.index(result["variable"])
      variable_name = result["variable"]
      if not final_results[key][variable_index] == "":
        if result["value"] != final_results[key][variable_index]:
          if not variable_name in multiples[key]:
            multiples[key][variable_name] = [final_results[key][variable_index]]
            final_results[key][variable_index] = "MULTIPLE"
            if not variable_name in supp_quals:
              supp_quals[variable_name] = 1
          multiples[key][variable_name].append(result["value"])
          if len(multiples[key][variable_name]) > supp_quals[variable_name]:
            supp_quals[variable_name] = len(multiples[key][variable_name])
      else:
        final_results[key][variable_index] = result["value"]
      #print("[%s] %s -> %s, multiples %s" % (key, result["variable"], final_results[key][variable_index], multiples[key]))

    # Derive AESTDY
    for key, result in final_results.items():
      usubjid_index = column_names.index('USUBJID')
      stdtc_index = column_names.index(self.name+'STDTC')
      stdy_index = column_names.index(self.name+'STDY')
      if stdtc_index and stdy_index:
        ref_date = next((item for item in reference_dates if item["USUBJID"] == result[usubjid_index]), None)
        if ref_date and result[stdtc_index]:
          result[stdy_index] = self.sdtm_derive_dy(ref_date['reference_date'],result[stdtc_index])

    # Derive AEENDY
    for key, result in final_results.items():
      # print("deriving AEENDY")
      usubjid_index = column_names.index('USUBJID')
      endtc_index = column_names.index(self.name+'ENDTC')
      endy_index = column_names.index(self.name+'ENDY')
      if endtc_index and stdy_index:
        ref_date = next((item for item in reference_dates if item["USUBJID"] == result[usubjid_index]), None)
        if ref_date and result[endtc_index]:
          result[endy_index] = self.sdtm_derive_dy(ref_date['reference_date'],result[endtc_index])

    for supp_name, count in supp_quals.items():
      #print("Count: ", count)
      for i in range(1, count + 1):
        name = "%s%s" % (supp_name, i)
        column_names.append(name)
        #print("Index: ", column_names.index(name))
        for subject, items in multiples.items():
          final_results[subject].append("")
          if supp_name in items:
            #print("I: ", i)
            #print("Items: ", items[supp_name])
            if i <= len(items[supp_name]):
              final_results[subject][column_names.index(name)] = items[supp_name][i - 1]
              #print("[%s] %s -> %s" % (subject, name, items[supp_name][i - 1]))

    self.add_seq(final_results, column_names.index(self.name+"SEQ"), column_names.index("USUBJID"))


    df = pd.DataFrame(columns=column_names)
    # print(df.head())
    for subject, result in final_results.items():
      df.loc[len(df.index)] = result
    # print(df.head())
    return df

  def construct_findings_dataframe(self, results):
    # topic = self.topic()
    topic = self.name+"TESTCD"
    topic_label = self.name+"TEST"
    result_var = self.name+"ORRES"
    seq_var = self.name+"SEQ"
    baseline_var = self.name+"BLFL"
    column_names = self.variable_list()
    column_names = [c for c in column_names if not c in ['VSCAT','VSSCAT','VSSTAT','VSREASND','VSLAT']]
    column_names.append('dp_uri')
    # Get reference dates
    reference_dates = self.get_reference_start_dates()

    final_results = {}
    for result in results:
      if 'TPT' in result.keys():
        key = "%s.%s.%s.%s" % (result['USUBJID'],result['test_code'], result['VISIT'], result['TPT'])
      else:
        key = "%s.%s.%s" % (result['USUBJID'],result['test_code'], result['VISIT'])
      if not key in final_results:
        record = [""] * len(column_names)
        record[column_names.index("STUDYID")] = result["STUDYID"]
        record[column_names.index("DOMAIN")] = result["DOMAIN"]
        # record[column_names.index("USUBJID")] = "%s.%s" % (result["STUDYID"], result["SUBJID"])
        record[column_names.index("USUBJID")] = result["USUBJID"]
        record[column_names.index(topic)] = result["test_code"]
        record[column_names.index(topic_label)] = result["test_label"]
        if 'VISIT' in result.keys():
          if result["VISIT"]:
            record[column_names.index("VISIT")] = result["VISIT"]
          else:
            record[column_names.index("VISIT")] = "Unplanned"
        if 'VISITNUM' in result.keys() and result["VISITNUM"]:
          record[column_names.index("VISITNUM")] = result["VISITNUM"]
        if 'VISITDY' in result.keys():
          dt = pd.Timedelta(result["VISITDY"])
          if pd.isnull(dt):
            pass
          else:
            planned_dy = dt.days*-1 if result['baseline_timing'] == "Before" else dt.days
            record[column_names.index("VISITDY")] = planned_dy
            # record[column_names.index("VISITDY")] = dt.days
        if 'TPT' in result.keys() and result["TPT"]:
          record[column_names.index(self.name+"TPT")] = result["TPT"]
        if 'EPOCH' in result.keys() and result["EPOCH"]:
          record[column_names.index("EPOCH")] = result["EPOCH"]
        if result['baseline_timing'] == "Fixed Reference":
          # print("setting baseline var",column_names.index(baseline_var))
          record[column_names.index(baseline_var)] = "Y"
        final_results[key] = record
      else:
        record = final_results[key]

      if result["variable"] == self.name+"DTC":
        ref_date = next((item for item in reference_dates if item["USUBJID"] == result['USUBJID']), None)
        if ref_date:
          if self.name+'DY' in column_names:
            index_dy = [column_names.index(self.name+'DY')][0]
            record[index_dy] = self.sdtm_derive_dy(ref_date['reference_date'],result['value'])

      variable_index = [column_names.index(result["variable"])][0]
      record[variable_index] = result["value"]
      if 'dp_uri' in result and result['variable'] == result_var:
        dp_uri_index = column_names.index('dp_uri')
        record[dp_uri_index] = result["dp_uri"]
    self.add_seq(final_results, column_names.index(seq_var), column_names.index("USUBJID"))
    df = pd.DataFrame(columns=column_names)
    for key, result in final_results.items():
      df.loc[len(df.index)] = result
    return df

  def construct_intervention_dataframe(self, results):
    # topic = self.topic()
    topic = self.name+"TRT"
    # topic_label = self.name+"TEST"
    seq_var = self.name+"SEQ"
    # baseline_var = self.name+"BLFL"
    result_var = self.name+"DOSE"
    column_names = self.variable_list()
    # Fix order of VISIT, VISITNUM
    column_names = [c for c in column_names if not c in ['EXLOC','EXTPT','EXLOT','EXLAT','EXDIR','EXSCAT','EXLNKGRP','EXLNKID','EXDOSTXT','EXDOSRGM','EXFAST','EXADJ','EXCAT']]
    column_names.remove('VISITNUM')
    column_names.insert(column_names.index('EPOCH'),'VISITNUM')
    column_names.remove('VISIT')
    column_names.insert(column_names.index('EPOCH'),'VISIT')
    column_names.append('dp_uri')
    # Get reference dates
    reference_dates = self.get_reference_start_dates()

    final_results = {}
    for result in results:
      key = "%s.%s.%s" % (result['USUBJID'],result['trt'], result['VISIT'])
      if not key in final_results:
        record = [""] * len(column_names)
        record[column_names.index("STUDYID")] = result["STUDYID"]
        record[column_names.index("DOMAIN")] = result["DOMAIN"]
        # record[column_names.index("USUBJID")] = "%s.%s" % (result["STUDYID"], result["SUBJID"])
        record[column_names.index("USUBJID")] = result["USUBJID"]
        record[column_names.index(topic)] = result["trt"]
        if 'VISIT' in column_names and 'VISIT' in result.keys():
          if result["VISIT"]:
            record[column_names.index("VISIT")] = result["VISIT"]
          else:
            record[column_names.index("VISIT")] = "Unplanned"
        if 'VISITNUM' in column_names and 'VISITNUM' in result.keys() and result["VISITNUM"]:
          record[column_names.index("VISITNUM")] = result["VISITNUM"]
        if 'VISITNUM' in column_names and 'VISITDY' in result.keys():
          dt = pd.Timedelta(result["VISITDY"])
          if pd.isnull(dt):
            pass
          else:
            planned_dy = dt.days*-1 if result['baseline_timing'] == "Before" else dt.days
            record[column_names.index("VISITDY")] = planned_dy
            # record[column_names.index("VISITDY")] = dt.days
        if 'TPT' in result.keys() and result["TPT"]:
          record[column_names.index(self.name+"TPT")] = result["TPT"]
        if 'EPOCH' in column_names and 'EPOCH' in result.keys() and result["EPOCH"]:
          record[column_names.index("EPOCH")] = result["EPOCH"]
        # if result['baseline_timing'] == "Fixed Reference":
        #   # print("setting baseline var",column_names.index(baseline_var))
        #   record[column_names.index(baseline_var)] = "Y"
        final_results[key] = record
      else:
        record = final_results[key]

      if result["variable"] == self.name+"STDTC":
        ref_date = next((item for item in reference_dates if item["USUBJID"] == result['USUBJID']), None)
        if ref_date:
          if self.name+'STDY' in column_names:
            index_dy = [column_names.index(self.name+'STDY')][0]
            record[index_dy] = self.sdtm_derive_dy(ref_date['reference_date'],result['value'])
      if result["variable"] == self.name+"ENDTC":
        ref_date = next((item for item in reference_dates if item["USUBJID"] == result['USUBJID']), None)
        if ref_date:
          if self.name+'STDY' in column_names:
            index_dy = [column_names.index(self.name+'ENDY')][0]
            record[index_dy] = self.sdtm_derive_dy(ref_date['reference_date'],result['value'])

      if 'dp_uri' in result and result['variable'] == result_var:
        dp_uri_index = column_names.index('dp_uri')
        record[dp_uri_index] = result["dp_uri"]

      variable_index = [column_names.index(result["variable"])][0]
      record[variable_index] = result["value"]
    self.add_seq(final_results, column_names.index(seq_var), column_names.index("USUBJID"))
    if self.name+"STDTC" in column_names and self.name+"ENDTC" in column_names and self.name+"DUR" in column_names:
      self.add_duration_dict(final_results, column_names.index(self.name+"DUR"), column_names.index(self.name+"STDTC"), column_names.index(self.name+"ENDTC"))

    df = pd.DataFrame(columns=column_names)
    for key, result in final_results.items():
      df.loc[len(df.index)] = result
    return df

  def variable_list(self):
    results = []
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:Domain)-[:VARIABLE_REL]->(sv:Variable) WHERE sd.uuid = "%s"
        RETURN sv.name as name ORDER BY toInteger(sv.ordinal)
      """ % (self.uuid)
      result = session.run(query)
      for record in result:
        name = record['name']
        if not self.hide_variable(name):
          results.append(name)
    return results

  def hide_variable(self, name):
    # TODO This is a quick fix, needs enable/disable flag on Study Domain 
    # "--TPT" needed for VS
    domain_hide_list = [
      "--GRPID", "--REFID", "--SPID", "--NAM",	"--LOINC", "--ANMETH", "--TMTHSN", "--LOBXFL",
      "--DRVFL", "--TOX", "--TOXGR", "--CLSIG",
      "--TPTNUM", "--ELTM", "--TPTREF", "--RFTDTC", "--PTFL", "--PDUR",
      "--TSTCND", "--BDAGNT", "--TSTOPO", "--STRESC", "--STRESN", "--STRESU"
    ]
    # "RFXSTDTC", "RFXENDTC",
    full_hide_list = [
      "TAETORD", "RFSTDTC", "RFENDTC", "RFCSTDTC", "RFCENDTC", "RFPENDTC"
    ]
    if name in full_hide_list:
      return True
    local_name = name.replace(self.name, '--', 1)
    if local_name in domain_hide_list:
      return True
    return False

  # def topic(self):
  #   db = Neo4jConnection()
  #   with db.session() as session:
  #     query = """MATCH (sd:StudyDomainInstance)-[:HAS_VARIABLE]->(sv:StudyVariable) WHERE sd.uuid = "%s" and sv.role="Topic"
  #       RETURN sv.name as name 
  #     """ % (self.uuid)
  #     result = session.run(query)
  #     for record in result:
  #       return record["name"]
  #   return None

  def print_dataframe(self, title, df):
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    print(title)
    print("")
    print(df)
    print("")
    print("")
    print("")