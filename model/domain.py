import pandas as pd
from typing import List
from model.base_node import BaseNode
from model.variable import Variable
from model.biomedical_concept import BiomedicalConceptSimple
from d4kms_service import Neo4jConnection
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
        return {'0':{'To be':'A DS domain'}}
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
        if self.name == "DM":
          record['SUBJID'] = row["SUBJECT"]
          record['SITEID'] = row["SITEID"]
          if 'COUNTRY' in row.keys():
            record['COUNTRY'] = row["COUNTRY"] 
          if 'INVNAM' in row.keys():
            record['INVNAM'] = row["INVNAM"]
          if 'INVID' in row.keys():
            record['INVID'] = row["INVID"]
        else:
          record['test_code'] = row['test_code']
          if 'VISIT' in row.keys():
            record['VISIT'] = row["VISIT"]
          if 'TPT' in row.keys():
            record['TPT'] = row["TPT"]
          if 'EPOCH' in row.keys():
            record['EPOCH'] = row["EPOCH"]
        results.append(record)
      #print ("RESULTS:", results)
      if self.name == "DM":
        df = self.construct_dm_dataframe(results)
      elif self.name == "DS":
        print("Creating DS dataframe")
        df = self.construct_ds_dataframe(results)
      else:
        df = self.construct_findings_dataframe(results)
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
      , e.label as VISIT
      , epoch.label as EPOCH
    """ % (self.uuid,self.uuid)
    print(query)
    return query

  def ds_query(self):
    # ONLY PLACE HOLDER RIGHT NOW
    query = "TO BE COMPLETED"
    print(query)
    return query

  def findings_query(self):
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
        ,visit as VISIT
        ,duration(TP['Main Timeline']) as ord
        ,TP[timelines] as TPT
        ,epoch[0] as  EPOCH 
        ,bc.uri as uuid
      order by DOMAIN, USUBJID, test_code, e_order,ord ,VISIT, TPT
    """ % (self.uuid)
    return query

  def convert_str_datetime(self, date_time_str):
    # parser.parse is said to be able to handle incomplete dates, but might need to accomodate
    return parser.parse(date_time_str)

  def sdtm_derive_age(self,rficdtc,brthdtc):
    inclusion_date = self.convert_str_datetime(rficdtc)
    birth_date = self.convert_str_datetime(brthdtc)
    # Note. Formula is using the fact that Python can subtract boolean from integer. (True = 1 and False = 0)
    age = inclusion_date.year - birth_date.year - ((inclusion_date.month, inclusion_date.day) < (birth_date.month, birth_date.day))
    return age

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
      if not result["SUBJID"] in final_results:
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

    if derive_age:
      index_rficdtc = [column_names.index('RFICDTC')][0]
      index_brthdtc = [column_names.index('BRTHDTC')][0]
      index_age = [column_names.index('AGE')][0]
      print('index_rficdtc',index_rficdtc)
      for key,vars in final_results.items():
        vars[index_age] = self.sdtm_derive_age(vars[index_rficdtc],vars[index_brthdtc])
        # vars[index_age] = derive_age(vars[index_rficdtc],vars[index_brthdtc])


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

  def construct_findings_dataframe(self, results):
    # topic = self.topic()
    topic = self.name+"TESTCD"
    column_names = self.variable_list()
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
        record[column_names.index("VISIT")] = result["VISIT"]
        if 'TPT' in result.keys():
          record[column_names.index(self.name+"TPT")] = result["TPT"]
        if 'EPOCH' in result.keys():
          record[column_names.index("EPOCH")] = result["EPOCH"]
        final_results[key] = record
      else:
        record = final_results[key]
      variable_index = [column_names.index(result["variable"])][0]
      record[variable_index] = result["value"]
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