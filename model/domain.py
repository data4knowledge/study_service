import pandas as pd
from typing import List
from model.base_node import BaseNode
from model.variable import Variable
from d4kms_service import Neo4jConnection

class Domain(BaseNode):
  uri: str
  name: str
  label: str
  structure: str
  ordinal: int
  items: List[Variable] = []
  bc_references: List[str] = []

  def data(self):
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      if self.name == "DM":
        query = self.dm_query()
      else:
        query = self.findings_query()
      print(query)
      rows = session.run(query)
      # print("rows",rows)
      for row in rows:
        print("looping rows",row)
        print("1",list(row.keys()))
        record = { 
          # 'uuid': row["uuid"], 
          'variable': row["variable"], 
          'value': row["value"], 
          'DOMAIN': row["DOMAIN"], 
          'STUDYID': row["STUDYID"], 
          'USUBJID': row["USUBJID"],
        }
        # record = {}
        # record['uuid'] = row["uuid"]
        # record['variable'] = row["variable"]
        # record['value'] = row["value"]
        # record['DOMAIN'] = row["DOMAIN"]
        # record['STUDYID'] = row["STUDYID"]
        # record['USUBJID'] = row["USUBJID"]
        if 'uuid' in row.keys():
          record['uuid'] = row['uuid']

        if self.name == "DM":
          record['SUBJID'] = row["SUBJECT"]
          record['SITEID'] = row["SITEID"]
          # record['COUNTRY'] = row["COUNTRY"] 
          # record['INVNAM'] = row["INVNAM"]
          # record['INVID'] = row["INVID"]
        else:
          if 'test_code' in row.keys():
            record['test_code'] = row['test_code']
          if 'VISIT' in row.keys():
            record['VISIT'] = row["VISIT"]
          if 'TPT' in row.keys():
            record['TPT'] = row["TPT"]
          if 'EPOCH' in row.keys():
            record['EPOCH'] = row["EPOCH"]

        print(2,"record",list(record.keys()))
        results.append(record)
      #print ("RESULTS:", results)
      if self.name == "DM":
        df = self.construct_dm_dataframe(results)
      else:
        df = self.construct_findings_dataframe(results)
      # result = df.to_dict('index')
      result = df.to_dict('records')
      return result

  def dm_query(self):
    # TODO Add in enabled true for USING BC relationship
    #  WITH DISTINCT bc, bcr1, sd, sv, fdt, bdt, sdp, wfi
    #  OPTIONAL MATCH (sv)-[:BC_RESTRICTION]->(bcr2:BiomedicalConceptRef) WHERE IS NULL bcr2 OR bcr1.uuid = bcr2.uuid
    # query = """
    #   MATCH (std:StudyDesign)-[]->(sd:StudyDomainInstance {uuid: '%s'})
    #   WITH std,sd
    #   MATCH (std)<-[]-(s:Study)-[:STUDY_IDENTIFIER]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE]->(o:Organisation)-[:ORGANISATION_TYPE]->(c:Code {decode: 'Clinical Study Sponsor'})
    #   WITH si, std, sd
    #   MATCH (wfi:WorkflowItem)-[:HAS_STUDY_BC_INSTANCE]->(bc:StudyBCInstance)<-[:BC_REF]-(bcr1:BiomedicalConceptRef)<-[:USING_BC]-(sd)-[:HAS_VARIABLE]->(sv:StudyVariable)
    #     -[:CLINICAL_RECORDING_REF]->(fdt:ClinicalRecordingRef)<-[:CLINICAL_RECORDING_REF]-(bdt:StudyBCDataTypeProperty)
    #     <-[:FOR_DATA_POINT]-(sdp:DataPoint)
    #   WITH DISTINCT si, std, sd, bc, sv, fdt, bdt, sdp, wfi 
    #   MATCH (wfi)-[:WORKFLOW_ITEM_ENCOUNTER]->(v:Encounter)<-[]-(e:StudyEpoch)
    #   WITH DISTINCT si, std, sd, bc, sv, fdt, bdt, sdp, wfi, v, e
    #   MATCH (sdp)-[:FOR_SUBJECT]->(subj:Subject)-[:PARTICIPATES_IN]->(std)-[]->(sd)
    #   WITH DISTINCT si, std, sd, bc, sv, fdt, bdt, sdp, wfi, v, e, subj
    #   MATCH (subj)-[:AT_SITE]->(site:Site)<-[:WORKS_AT]-(inv:Investigator)
    #   WITH DISTINCT si, std, sd, bc, sv, fdt, bdt, sdp, wfi, v, e, subj, site, inv
    #   MATCH (ct:ValueSet)<-[:HAS_RESPONSE]-()<-[:HAS_STUDY_BC_DATA_TYPE]-(StudyBCItem {name: "Test"})<-[:HAS_STUDY_BC_ITEM]-(bc)-[*]->(bdt)
    #   RETURN DISTINCT sd.name as domain, sv.name as variable, sdp.value as data, wfi.uuid as uuid, v.encounterName as visit, e.studyEpochName as epoch, 
    #     subj.identifier as subject, ct.notation as test_code, site.identifier as siteid, 
    #     inv.name as invnam, inv.identifier as invid, site.country_code as country, si.studyIdentifier as studyid ORDER BY subject

    query = """
      MATCH (sdp:DataPoint)-[:FOR_DATA_POINT]->(bdtp:StudyBCDataTypeProperty)<-[]-(bdt:StudyBCDataType)<-[]-(bi:StudyBCItem)<-[]-(bc:StudyBCInstance)
        <-[:BC_REF]-(bcr:BiomedicalConceptRef)<-[:USING_BC]-(sdi:StudyDomainInstance {uuid: '%s'})
      WITH DISTINCT sdp, bdtp, bdt, bi, bc, bcr, sdi
      MATCH (sdi)-[:HAS_VARIABLE]->(sv:StudyVariable)-[:CLINICAL_RECORDING_REF]->(crr:ClinicalRecordingRef)<-[:CLINICAL_RECORDING_REF]-(bdtp)

      WITH DISTINCT sdp, bdtp, bdt, bi, bc, bcr, sdi, sv, crr
      MATCH (sv)-[:BC_RESTRICTION]->(bcr2:BiomedicalConceptRef)-[:BC_REF]->(bc2:StudyBCInstance) WHERE bc.name=bc2.name

      WITH DISTINCT sdp, bdtp, bdt, bi, bc, bcr, sdi, sv, crr
      MATCH (bc)<-[:HAS_STUDY_BC_INSTANCE]-(wfi:WorkflowItem)-[:WORKFLOW_ITEM_ENCOUNTER]->(v:Encounter)<-[]-(e:StudyEpoch)
      WITH DISTINCT sdp, bdtp, bdt, bi, bc, bcr, sdi, sv, crr, wfi, v, e
      MATCH (sdp)-[:FOR_SUBJECT]->(subj:Subject)-[:PARTICIPATES_IN]->(sd:StudyDesign)-[]->(sdi)
      WITH DISTINCT sdp, bdtp, bdt, bi, bc, bcr, sdi, sv, crr, wfi, v, e, subj, sd
      MATCH (sd)<-[]-(s:Study)-[:STUDY_IDENTIFIER]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE]->(o:Organisation)-[:ORGANISATION_TYPE]->
        (c:Code {decode: 'Clinical Study Sponsor'})
      WITH DISTINCT sdp, bdtp, bdt, bi, bc, bcr, sdi, sv, crr, wfi, v, e, subj, sd, si
      MATCH (subj)-[:AT_SITE]->(site:Site)<-[:WORKS_AT]-(inv:Investigator)
      WITH DISTINCT sdp, bdtp, bdt, bi, bc, bcr, sdi, sv, crr, wfi, v, e, subj, sd, si, site, inv
      MATCH (bc)-[:HAS_STUDY_BC_ITEM]->(StudyBCItem {name: "Test"})-[:HAS_STUDY_BC_DATA_TYPE]->()-[:HAS_RESPONSE]->(ct:ValueSet)
      RETURN DISTINCT sdi.name as domain, sv.name as variable, sdp.value as data, bc.uuid as uuid, v.encounterName as visit, e.studyEpochName as epoch, 
        subj.identifier as subject, ct.notation as test_code, site.identifier as siteid, 
        inv.name as invnam, inv.identifier as invid, site.country_code as country, si.studyIdentifier as studyid ORDER BY subject LIMIT 2000
    """ % (self.uuid)
    # Query as vertical findings
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
    """ % (self.uuid)
    return query

  def findings_query(self):
    # query = """
    #   MATCH (sdp:DataPoint)-[:FOR_DATA_POINT]->(bdtp:StudyBCDataTypeProperty)<-[]-(bdt:StudyBCDataType)<-[]-(bi:StudyBCItem)<-[]-(bc:StudyBCInstance)
    #     <-[:BC_REF]-(bcr:BiomedicalConceptRef)<-[:USING_BC]-(sdi:StudyDomainInstance {uuid: '%s'})
    #   WITH DISTINCT sdp, bdtp, bdt, bi, bc, bcr, sdi
    #   MATCH (sdi)-[:HAS_VARIABLE]->(sv:StudyVariable)-[:CLINICAL_RECORDING_REF]->(crr:ClinicalRecordingRef)<-[:CLINICAL_RECORDING_REF]-(bdtp)
    #   WITH DISTINCT sdp, bdtp, bdt, bi, bc, bcr, sdi, sv, crr
    #   MATCH (bc)<-[:HAS_STUDY_BC_INSTANCE]-(wfi:WorkflowItem)-[:WORKFLOW_ITEM_ENCOUNTER]->(v:Encounter)<-[]-(e:StudyEpoch)
    #   WITH DISTINCT sdp, bdtp, bdt, bi, bc, bcr, sdi, sv, crr, wfi, v, e
    #   MATCH (sdp)-[:FOR_SUBJECT]->(subj:Subject)-[:PARTICIPATES_IN]->(sd:StudyDesign)-[]->(sdi)
    #   WITH DISTINCT sdp, bdtp, bdt, bi, bc, bcr, sdi, sv, crr, wfi, v, e, subj, sd
    #   MATCH (sd)<-[]-(s:Study)-[:STUDY_IDENTIFIER]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE]->(o:Organisation)-[:ORGANISATION_TYPE]->
    #     (c:Code {decode: 'Clinical Study Sponsor'})
    #   WITH DISTINCT sdp, bdtp, bdt, bi, bc, bcr, sdi, sv, crr, wfi, v, e, subj, sd, si
    #   MATCH (subj)-[:AT_SITE]->(site:Site)<-[:WORKS_AT]-(inv:Investigator)
    #   WITH DISTINCT sdp, bdtp, bdt, bi, bc, bcr, sdi, sv, crr, wfi, v, e, subj, sd, si, site, inv
    #   MATCH (bc)-[:HAS_STUDY_BC_ITEM]->(StudyBCItem {name: "Test"})-[:HAS_STUDY_BC_DATA_TYPE]->()-[:HAS_RESPONSE]->(ct:ValueSet)
    #   RETURN DISTINCT sdi.name as domain, sv.name as variable, sdp.value as data, bc.uuid as uuid, v.encounterName as visit, e.studyEpochName as epoch, 
    #     subj.identifier as subject, ct.notation as test_code, site.identifier as siteid, 
    #     inv.name as invnam, inv.identifier as invid, site.country_code as country, si.studyIdentifier as studyid ORDER BY subject LIMIT 2000
    # """ % (self.uuid)
    # Query as vertical findings
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
      limit 10
    """ % (self.uuid)
    print("query",query)
    return query

  def construct_dm_dataframe(self, results):
    multiples = {}
    supp_quals = {}
    column_names = self.variable_list()
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
        # final_results[key][column_names.index("INVID")] = result["INVID"]
        # final_results[key][column_names.index("INVNAM")] = result["INVNAM"]
        # final_results[key][column_names.index("COUNTRY")] = result["COUNTRY"]
      variable_index = [column_names.index(result["variable"])][0]
      variable_name = result["variable"]
      #print("Index:", variable_index)
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
    for subject, result in final_results.items():
      df.loc[len(df.index)] = result
    return df

  def construct_findings_dataframe(self, results):
    # topic = self.topic()
    topic = self.name+"TESTCD"
    print("self",self)
    column_names = self.variable_list()
    print(3,"column_names",column_names)
    final_results = {}
    for result in results:
      print(3,"result",list(result.keys()))

      # key = "%s.%s" % (result['SUBJID'], result['uuid'])
      key = "%s.%s.%s.%s" % (result['USUBJID'],result['test_code'], result['VISIT'], result['TPT'])
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
        record[column_names.index("EPOCH")] = result["EPOCH"]
        final_results[key] = record
      else:
        record = final_results[key]
      variable_index = [column_names.index(result["variable"])][0]
      record[variable_index] = result["value"]
      print(4,"record",record)
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
    # "--TPT", 
    domain_hide_list = [
      "--GRPID", "--REFID", "--SPID", "--NAM",	"--LOINC", "--ANMETH", "--TMTHSN", "--LOBXFL",
      "--DRVFL", "--TOX", "--TOXGR", "--CLSIG",
      "--TPTNUM", "--ELTM", "--TPTREF", "--RFTDTC", "--PTFL", "--PDUR",
      "--TSTCND", "--BDAGNT", "--TSTOPO", "--STRESC", "--STRESN", "--STRESU"
    ]
    full_hide_list = [
      "TAETORD", "RFSTDTC", "RFENDTC", "RFXSTDTC", "RFXENDTC", "RFCSTDTC", "RFCENDTC", "RFICDTC", "RFPENDTC"
    ]
    if name in full_hide_list:
      return True
    local_name = name.replace(self.name, '--', 1)
    if local_name in domain_hide_list:
      return True
    return False

  def topic(self):
    db = Neo4jConnection()
    with db.session() as session:
      query = """MATCH (sd:StudyDomainInstance)-[:HAS_VARIABLE]->(sv:StudyVariable) WHERE sd.uuid = "%s" and sv.role="Topic"
        RETURN sv.name as name 
      """ % (self.uuid)
      result = session.run(query)
      for record in result:
        return record["name"]
    return None

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