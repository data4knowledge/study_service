import pandas as pd
from typing import List
from model.base_node import BaseNode
from model.variable import Variable
from model.biomedical_concept import BiomedicalConceptSimple
from service.ct_service import CTService
from d4kms_service import Neo4jConnection
from d4kms_generic import application_logger
from dateutil import parser 
class Domain(BaseNode):
  uri: str
  name: str
  label: str
  structure: str
  ordinal: int
  items: List[Variable] = []
  bc_references: List[str] = []

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
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      if self.name == "DM":
        query = self.dm_query()
      elif self.name == "DS":
        query = self.ds_query()
        metadata = self.get_ds_metadata()
        # for x in metadata:
        #   print("metadata",metadata)
      else:
        query = self.findings_query()
      # print(query)
      rows = session.run(query)
      for row in rows:
        record = { 
          'variable': row["variable"], 
          'value': row["value"], 
          'DOMAIN': row["DOMAIN"], 
          'STUDYID': row["STUDYID"], 
          'USUBJID': row["USUBJID"],
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
        if self.name == "DM":
          record['SUBJID'] = row["SUBJECT"]
          record['SITEID'] = row["SITEID"]
          if 'COUNTRY' in row.keys():
            record['COUNTRY'] = row["COUNTRY"] 
          if 'INVNAM' in row.keys():
            record['INVNAM'] = row["INVNAM"]
          if 'INVID' in row.keys():
            record['INVID'] = row["INVID"]
        elif self.name == "DS":
          # Add metadata
          items = [item for item in metadata if row['bc_uuid'] == item['bc_uuid']]
          for item in items:
            record[item['variable']] = item['value']
          for variable in ['EPOCH','DSDTC']:
            if variable in row.keys():
              record[variable] = row[variable]
        else:
          record['test_code'] = row['test_code']
        results.append(record)
      # for x in results[0:5]:
      #   print(x)
      if self.name == "DM":
        df = self.construct_dm_dataframe(results)
      elif self.name == "DS":
        print("Creating DS dataframe")
        # for x in results:
        #   print(x)
        df = self.construct_ds_dataframe(results)
      else:
        metadata = self.get_findings_metadata(results)
        metadata = {}
        df = self.construct_findings_dataframe(results,metadata)
      result = df.to_dict('index')
      # result = df.to_dict('records')
      return result

  def dm_query(self):
    # Query as vertical findings. Now with RFICDTC
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
      MATCH (domain)-[:VARIABLE_REL]->(var:Variable)
      MATCH (dc)-[:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(tim:Timing)
      MATCH (act_inst_main)-[:ENCOUNTER_REL]->(e:Encounter)
      MATCH (act_inst_main)-[:EPOCH_REL]->(epoch:StudyEpoch)
      WHERE  var.label = bcp.label
      return
      si.studyIdentifier as STUDYID
      , domain.name as DOMAIN
      , subj.identifier as USUBJID
      , right(subj.identifier,6) as SUBJECT
      , var.name as variable
      , dp.value as value
      , site.name as SITEID
      , e.name as VISITNUM
      , e.label as VISIT
      , epoch.label as EPOCH
      union
      MATCH (sd:StudyDesign)-[:DOMAIN_REL]->(domain:Domain {uuid:'%s'})
      MATCH (sd)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)
      MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(sis:Organization {name:'Eli Lilly'})
      MATCH (domain)-[:USING_BC_REL]-(bc:BiomedicalConcept {name: "Informed Consent Obtained"})-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty {name:'DSSTDTC'})
      MATCH (bcp)<-[:PROPERTIES_REL]-(dc:DataContract)<-[:FOR_DC_REL]-(dp:DataPoint)-[:FOR_SUBJECT_REL]->(subj:Subject)
      MATCH (subj)-[:ENROLLED_AT_SITE_REL]->(site:StudySite)
      MATCH (dc)-[:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(tim:Timing)
      MATCH (act_inst_main)-[:ENCOUNTER_REL]->(e:Encounter)
      MATCH (act_inst_main)-[:EPOCH_REL]->(epoch:StudyEpoch)
      return
      si.studyIdentifier as STUDYID
      , domain.name as DOMAIN
      , subj.identifier as USUBJID
      , right(subj.identifier,6) as SUBJECT
      , 'RFICDTC' as variable
      , dp.value as value
      , site.name as SITEID
      , e.name as VISITNUM
      , e.label as VISIT
      , epoch.label as EPOCH
    """ % (self.uuid,self.uuid)
    print(query)
    return query

  def ds_query(self):
    # ONLY PLACE HOLDER RIGHT NOW
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
      MATCH (domain)-[:VARIABLE_REL]->(var:Variable)
      MATCH (dc)-[:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(tim:Timing)
      MATCH (act_inst_main)-[:ENCOUNTER_REL]->(e:Encounter)
      MATCH (act_inst_main)-[:EPOCH_REL]->(epoch:StudyEpoch)
      // WHERE  var.label = bcp.label
      WHERE  var.name = bcp.name
      return
            si.studyIdentifier as STUDYID
            , domain.name as DOMAIN
            , subj.identifier as USUBJID
            , right(subj.identifier,6) as SUBJECT
            , var.name as variable
            , dp.value as value
            , site.name as SITEID
            , e.label as VISIT
            , epoch.label as EPOCH
            , bc.uuid as bc_uuid
    """ % (self.uuid)
    # print(query)
    return query

  def findings_query(self):
    # Query with timing.value as VISITDY
    alt_query = """
      match(domain:Domain{uuid:'%s'})-[:VARIABLE_REL]->(var:Variable)-[:IS_A_REL]->(crm:CRMNode)<-[:IS_A_REL]-(bc_prop:BiomedicalConceptProperty), (domain)-[:USING_BC_REL]->(bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bc_prop)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(tim:Timing)-[:TYPE_REL]->(tim_ref:Code)
      OPTIONAL MATCH(act_inst_main)-[:ENCOUNTER_REL]->(e:Encounter),(act_inst_main)-[:EPOCH_REL]->(epoch:StudyEpoch)
      OPTIONAL MATCH(act_inst_main)<-[:INSTANCES_REL]-(tl:ScheduleTimeline)
      MATCH (domain)<-[:DOMAIN_REL]-(sd:StudyDesign)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(sis:Organization {name:'Eli Lilly'})
      MATCH (dc)<-[:FOR_DC_REL]-(d:DataPoint)-[:FOR_SUBJECT_REL]->(s:Subject)
      MATCH (bc)-[:CODE_REL]->()-[:STANDARD_CODE_REL]-(code:Code)
      WITH code.decode as test_code, si,domain, collect(epoch.name) as epoch,collect(toInteger(split(e.id,'_')[1])) as e_order,var, bc, dc, d, s, collect(e.label) as vis, apoc.map.fromPairs(collect([tl.label,tim.value])) as TP, tim_ref.decode as tim_ref
      WITH test_code, si,domain, epoch,e_order[0] as e_order,var, bc, dc, d, s,apoc.text.join(apoc.coll.remove(keys(TP),apoc.coll.indexOf(keys(TP),'Main Timeline')),',') as timelines, TP, apoc.text.join(vis,',') as visit, tim_ref
      RETURN 
      si.studyIdentifier as STUDYID
      ,domain.name as DOMAIN
      ,s.identifier as USUBJID
      ,test_code as test_code
      ,bc.name as TEST
      ,var.name as variable
      ,d.value as value
      ,e_order as VISITNUM
      ,visit as VISIT
      ,TP['Main Timeline'] as VISITDY
      ,tim_ref as baseline_timing
      ,duration(TP['Main Timeline']) as ord
      ,TP[timelines] as TPT
      ,epoch[0] as  EPOCH 
      ,bc.uri as uuid
      order by DOMAIN, USUBJID, test_code, e_order,ord ,VISIT, TPT
    """ % (self.uuid)
    return alt_query

    query = """
      match(domain:Domain{uuid:'%s'})-[:VARIABLE_REL]->(var:Variable)-[:IS_A_REL]->(crm:CRMNode)<-[:IS_A_REL]-(bc_prop:BiomedicalConceptProperty), (domain)-[:USING_BC_REL]->(bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bc_prop)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(tim:Timing)
      OPTIONAL MATCH(act_inst_main)-[:ENCOUNTER_REL]->(e:Encounter),(act_inst_main)-[:EPOCH_REL]->(epoch:StudyEpoch)
      OPTIONAL MATCH(act_inst_main)<-[:INSTANCES_REL]-(tl:ScheduleTimeline)
      MATCH (domain)<-[:DOMAIN_REL]-(sd:StudyDesign)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(sis:Organization {name:'Eli Lilly'})
      MATCH (dc)<-[:FOR_DC_REL]-(d:DataPoint)-[:FOR_SUBJECT_REL]->(s:Subject)
      MATCH (bc)-[:CODE_REL]->()-[:STANDARD_CODE_REL]-(code:Code)
      WITH code.decode as test_code, si,domain, collect(epoch.name) as epoch,collect(toInteger(split(e.id,'_')[1])) as e_order,var, bc, dc, d, s, collect(e.label) as vis, apoc.map.fromPairs(collect([tl.label,tim.value])) as TP
      WITH test_code, si,domain, epoch,e_order[0] as e_order,var, bc, dc, d, s,apoc.text.join(apoc.coll.remove(keys(TP),apoc.coll.indexOf(keys(TP),'Main Timeline')),',') as timelines, TP, apoc.text.join(vis,',') as visit
      RETURN 
        si.studyIdentifier as STUDYID
        ,domain.name as DOMAIN
        ,s.identifier as USUBJID
        ,test_code as test_code
        ,bc.name as TEST
        ,var.name as variable
        ,d.value as value
        ,e_order as VISITNUM
        ,visit as VISIT
        ,duration(TP['Main Timeline']) as ord
        ,TP[timelines] as TPT
        ,epoch[0] as  EPOCH 
        ,bc.uri as uuid
      order by DOMAIN, USUBJID, test_code, e_order,ord ,VISIT, TPT
    """ % (self.uuid)

    return query

  def convert_str_datetime(self, date_time_str):
    # parser.parse is said to be able to handle incomplete dates, but might need fixing
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

  def sdtm_derive_dy(self, ref_start_date_str, end_date_str):
    if ref_start_date_str and end_date_str:
      ref_start = self.convert_str_datetime(ref_start_date_str)
      dtc = self.convert_str_datetime(end_date_str)
      dy = dtc - ref_start
      # print("ref_start",ref_start,"dtc",dtc,"dy",dy)
      if ref_start > dtc:
        return dy.days
      else:
        return dy.days + 1
    return None

  def add_seq_dict(self, results, seq_index, usubjid_index):
    print("adding seq dict")
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

  def add_seq_list_of_dict(self, results, seq_var):
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

  def add_seq(self, results, seq_index = None, usubjid_index = None):
    seq_var = self.name + "SEQ"
    if isinstance(results, dict):
      self.add_seq_dict(results, seq_index, usubjid_index)
    elif isinstance(results, list) and isinstance(results[0], dict):
      self.add_seq_list_of_dict(results, seq_var)
    else:
        application_logger.info(f"Don't know how to add --SEQ to {results.__class__}")


  def get_reference_start_dates(self):
    db = Neo4jConnection()
    with db.session() as session:
      # FIX: QUERY TO BE UNIQUE FOR STUDY
      # FIX: QUERY SHOULD USE REFERENCE START DATE, NOT INFORMED CONSENT
      query = """
        MATCH (s:Subject)<-[:FOR_SUBJECT_REL]-(dp:DataPoint)-[:FOR_DC_REL]->(:DataContract)-[:PROPERTIES_REL]->(:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(:BiomedicalConcept {name:"Informed Consent Obtained"})
        return s.identifier as USUBJID, dp.value as consent_date
      """
      query = """
        MATCH (s:Subject)<-[:FOR_SUBJECT_REL]-(dp:DataPoint)-[:FOR_DC_REL]->(dc:DataContract)-[:INSTANCES_REL]->(sai:ScheduledActivityInstance)-[:ENCOUNTER_REL]->(e:Encounter)
        MATCH (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
        WHERE e.label = "Baseline"
        and bcp.name = "--DTC"
        return distinct s.identifier as USUBJID, dp.value as baseline_date
      """
      # print("reference start date query",query)
      results = session.run(query)
      return [result.data() for result in results]


  def construct_dm_dataframe(self, results):
    multiples = {}
    supp_quals = {}
    column_names = self.variable_list()
    # Check if age can be derived
    if all(key in column_names for key in ('RFICDTC','BRTHDTC')):
      derive_age = True
    # print("COLS:", column_names)
    final_results = {}
    for result in results:
      key = result["SUBJID"]
      if not result["SUBJID"] in final_results:  # Subject does not exist yet in result. Create default variables
        multiples[key] = {}
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
      variable_name = result["variable"]
      variable_index = [column_names.index(variable_name)][0]
      if not final_results[key][variable_index] == "": # Subject already have a value for variable
        print("  variable",variable_name,variable_index)
        print("  2.1 has previous value",final_results[key][variable_index])
        if result["value"] != final_results[key][variable_index]:
          print("  2.2",result["value"])
          if not variable_name in multiples[key]: # create "multiple"
            print("  2.3: variable_name:",variable_name)
            multiples[key][variable_name] = [final_results[key][variable_index]]
            final_results[key][variable_index] = "MULTIPLE"
            if not variable_name in supp_quals:
              supp_quals[variable_name] = 1
          multiples[key][variable_name].append(result["value"])
          if len(multiples[key][variable_name]) > supp_quals[variable_name]:
            supp_quals[variable_name] = len(multiples[key][variable_name])
      else: # Subject does not have a value for variable
        print("  3.1 no previous value",variable_name,"=",result['value'])
        final_results[key][variable_index] = result["value"]
        print("  3.9 adding next for SUBJID",final_results[key])
      print("[%s] %s -> %s, multiples %s" % (key, result["variable"], final_results[key][variable_index], multiples[key]))

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

  def get_ds_metadata(self):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign)-[:DOMAIN_REL]->(domain:Domain {uuid:'%s'})
        MATCH (sd)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)
        MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(sis:Organization {name:'Eli Lilly'})
        MATCH (domain)-[:USING_BC_REL]-(bc:BiomedicalConcept)
        WITH si, domain, bc
        MATCH (bc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bcp)-[:CODE_REL]->(ac:AliasCode)-[:STANDARD_CODE_REL]->(c:Code)-[:HAS_TERM]->(term:SkosConcept)
        MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)
        MATCH (domain)-[:VARIABLE_REL]->(var:Variable)
        WHERE  var.name = bcp.name
        return
              si.studyIdentifier as STUDYID
              , domain.name as DOMAIN
              , var.name as variable
              , term.notation as value
              , bc.uuid as bc_uuid
      """ % (self.uuid)
      print("ds metadata query",query)
      results = session.run(query)
      return [result.data() for result in results]

  def construct_ds_dataframe(self, results):
    multiples = {}
    supp_quals = {}
    column_names = self.variable_list()
    print("COLS:", column_names)
    # ['STUDYID', 'DOMAIN', 'USUBJID', 'DSSEQ', 'DSTERM', 'DSDECOD', 'DSCAT', 'DSSCAT', 'EPOCH', 'DSDTC', 'DSSTDTC', 'DSDY', 'DSSTDY']
    final_results = {}
    self.add_seq(results)
    for result in results:
      print("DS",result)
      if 'DSSEQ' in result.keys():
        key = "%s.%s" % (result['USUBJID'],result['DSSEQ'])
      else:
        key = "%s." % (result['USUBJID'])
      if not key in final_results:
        multiples[key] = {}
        final_results[key] = [""] * len(column_names)
        final_results[key][column_names.index("STUDYID")] = result["STUDYID"]
        final_results[key][column_names.index("DOMAIN")] = result["DOMAIN"]
        # final_results[key][column_names.index("USUBJID")] = "%s.%s" % (result["STUDYID"], result["SUBJID"])
        final_results[key][column_names.index("USUBJID")] = result["USUBJID"]
        if "DSSEQ" in result.keys():
          final_results[key][column_names.index("DSSEQ")] = result["DSSEQ"]
        if "DSTERM" in result.keys():
          final_results[key][column_names.index("DSTERM")] = result["DSTERM"]
        if "DSDECOD" in result.keys():
          final_results[key][column_names.index("DSDECOD")] = result["DSDECOD"]
        if "DSCAT" in result.keys():
          final_results[key][column_names.index("DSCAT")] = result["DSCAT"]
        # final_results[key][column_names.index("DSSCAT")] = result["DSSCAT"]
        if "EPOCH" in result.keys():
          print("EPOCH!!!",result["EPOCH"])
          final_results[key][column_names.index("EPOCH")] = result["EPOCH"]
        if "DSDTC" in result.keys():
          final_results[key][column_names.index("DSDTC")] = result["DSDTC"]
        if "DSSTDTC" in result.keys():
          final_results[key][column_names.index("DSSTDTC")] = result["DSSTDTC"]
        if "DSENDTC" in result.keys():
          final_results[key][column_names.index("DSENDTC")] = result["DSENDTC"]
      variable_index = [column_names.index(result["variable"])][0]
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

    df = pd.DataFrame(columns=column_names)
    # print(df.head())
    for subject, result in final_results.items():
      df.loc[len(df.index)] = result
    # print(df.head())
    return df

  def get_test_code_label(self, test_code):
    print("Finding label for ",test_code)
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        use `ct-service-dev`
        MATCH (m:SkosConcept)-[:NARROWER]->(n:SkosConcept) WHERE 
        n.notation='%s' 
        AND NOT EXISTS ((:SkosConcept)-[:PREVIOUS]->(m)) 
        RETURN n,m
      """ % (test_code)
      print("get test code identifier query",query)
      response = session.run(query)
      results = [result.data() for result in response]
      first_sdtm_label = next((cli for cli in results if 'Test Name' in cli['parent']['pref_label']),[])
      print("first_sdtm_label",first_sdtm_label)
    return 0

    # response = ct.find_notation(test_code)
    # # Find first SDTM result in response
    # first_sdtm_term = next((cli for cli in response if 'SDTM' in cli['parent']['pref_label']),[])
    # if first_sdtm_term:
    #   response = ct.find_by_identifier(first_sdtm_term['child']['identifier'])
    #   first_sdtm_label = next((cli for cli in response if 'Test Name' in cli['parent']['pref_label']),[])
    #   if first_sdtm_term:
    #     return first_sdtm_label['child']['pref_label']
    #   else:
    #     return f"No label for {first_sdtm_term['parent']['identifier']}.{first_sdtm_term['child']['identifier']}"
    # else:
    #   return "--"

  def get_findings_metadata(self,results):
    tests = {}
    test_codes = set([result['test_code'] for result in results])
    print("len(test_codes)",len(test_codes))
    for test_code in test_codes:
      test_label = self.get_test_code_label(test_code)
      # tests[test_code] = test_label
    # return tests


  def construct_findings_dataframe(self, results, tests):
    # topic = self.topic()
    topic = self.name+"TESTCD"
    topic_label = self.name+"TEST"
    seq_var = self.name+"SEQ"
    column_names = self.variable_list()
    # print('column_names',column_names)
    baseline_dates = self.get_reference_start_dates()
    # tests ={'SYSBP':'Systolic Blood Pressure'}

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
        # if result["test_code"] in tests:
        #   record[column_names.index(topic_label)] = tests[result["test_code"]]
        # else:
        #   test_label = self.get_test_code_label(result['test_code'])
        #   tests[result['test_code']] = test_label
        #   record[column_names.index(topic_label)] = test_label
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
            # print("not dt",result["VISITDY"],result["USUBJID"])
            # print("  ",result)
            # record[column_names.index("VISITDY")] = float("nan")
          else:
            planned_dy = dt.days*-1 if result['baseline_timing'] == "Before" else dt.days
            record[column_names.index("VISITDY")] = planned_dy
            # record[column_names.index("VISITDY")] = dt.days
        if 'TPT' in result.keys():
          record[column_names.index(self.name+"TPT")] = result["TPT"]
        if 'EPOCH' in result.keys() and result["EPOCH"]:
          record[column_names.index("EPOCH")] = result["EPOCH"]
        final_results[key] = record
      else:
        record = final_results[key]

      if result["variable"] == self.name+"DTC":
        baseline = next((item for item in baseline_dates if item["USUBJID"] == result['USUBJID']), None)
        if baseline:
          if self.name+'DY' in column_names:
            index_dy = [column_names.index(self.name+'DY')][0]
            record[index_dy] = self.sdtm_derive_dy(baseline['baseline_date'],result['value'])

      variable_index = [column_names.index(result["variable"])][0]
      record[variable_index] = result["value"]
    self.add_seq(final_results, column_names.index(seq_var), column_names.index("USUBJID"))
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
    full_hide_list = [
      "TAETORD", "RFSTDTC", "RFENDTC", "RFXSTDTC", "RFXENDTC", "RFCSTDTC", "RFCENDTC", "RFPENDTC"
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