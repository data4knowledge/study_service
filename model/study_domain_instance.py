from uuid import UUID
from typing import Union
from model.node import Node
from model.neo4j_connection import Neo4jConnection
import pandas as pd

class StudyDomainInstance(Node):
  uuid: Union[UUID, None] = None
  name: str

  def data(self):
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      # TODO Add in enabled true for USING BC relationship
      # TODO Add in stud identifier
      #  WITH DISTINCT bc, bcr1, sd, sv, fdt, bdt, sdp, wfi
      #  OPTIONAL MATCH (sv)-[:BC_RESTRICTION]->(bcr2:BiomedicalConceptRef) WHERE IS NULL bcr2 OR bcr1.uuid = bcr2.uuid
      query = """
        MATCH (std:StudyDesign)-[]->(sd:StudyDomainInstance {name: '%s'})
        WITH std,sd
        MATCH (std)<-[]-(s:Study)-[:STUDY_IDENTIFIER]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE]->(o:Organisation)-[:ORGANISATION_TYPE]->(c:Code {decode: 'Clinical Study Sponsor'})
        WITH si, std, sd
        MATCH (wfi:WorkflowItem)-[:HAS_STUDY_BC_INSTANCE]->(bc:StudyBCInstance)<-[:BC_REF]-(bcr1:BiomedicalConceptRef)<-[:USING_BC]-(sd)-[:HAS_VARIABLE]->(sv:StudyVariable)
          -[:CLINICAL_RECORDING_REF]->(fdt:ClinicalRecordingRef)<-[:CLINICAL_RECORDING_REF]-(bdt:StudyBCDataTypeProperty)
          <-[:FOR_DATA_POINT]-(sdp:DataPoint)
        WITH DISTINCT si, std, sd, bc, sv, fdt, bdt, sdp, wfi 
        MATCH (wfi)-[:WORKFLOW_ITEM_ENCOUNTER]->(v:Encounter)<-[]-(e:StudyEpoch)
        WITH DISTINCT si, std, sd, bc, sv, fdt, bdt, sdp, wfi, v, e
        MATCH (sdp)-[:FOR_SUBJECT]->(subj:Subject)-[:PARTICIPATES_IN]->(std)-[]->(sd)
        WITH DISTINCT si, std, sd, bc, sv, fdt, bdt, sdp, wfi, v, e, subj
        MATCH (subj)-[:AT_SITE]->(site:Site)<-[:WORKS_AT]-(inv:Investigator)
        WITH DISTINCT si, std, sd, bc, sv, fdt, bdt, sdp, wfi, v, e, subj, site, inv
        MATCH (ct:ValueSet)<-[:HAS_RESPONSE]-()<-[:HAS_STUDY_BC_DATA_TYPE]-(StudyBCItem {name: "Test"})<-[:HAS_STUDY_BC_ITEM]-(bc)-[*]->(bdt)
        RETURN DISTINCT sd.name as domain, sv.name as variable, sdp.value as data, wfi.uuid as uuid, v.encounterName as visit, e.studyEpochName as epoch, 
          subj.identifier as subject, ct.notation as test_code, site.identifier as siteid, 
          inv.name as invnam, inv.identifier as invid, site.country_code as country, si.studyIdentifier as studyid
      """ % (self.name)
      print(query)
      rows = session.run(query)
      for row in rows:
        record = { 
          'uuid': row["uuid"], 
          'variable': row["variable"], 
          'value': row["data"], 
          'DOMAIN': row["domain"], 
          'STUDYID': row["studyid"], 
          'SUBJID': row["subject"], 
          'SITEID': row["siteid"],
          'INVNAM': row["invnam"],
          'INVID': row["invid"],  
          'VISIT': row["visit"], 
          'EPOCH': row["epoch"],
          'COUNTRY': row["country"],  
          'test_code': row["test_code"], 
        }
        results.append(record)
        print ("RECORD:", record)
        df = self.construct_domain_dataframe(self.name, results)
        self.print_dataframe(self.name, df)
      return results

  def construct_domain_dataframe(self, domain, results):
    multiples = {}
    supp_quals = {}
    column_names = self.variable_list(domain)
    final_results = {}
    for result in results:
      key = result["SUBJID"]
      if not result["SUBJID"] in final_results:
        multiples[key] = {}
        final_results[key] = [""] * len(column_names)
        final_results[key][column_names.index("STUDYID")] = result["STUDYID"]
        final_results[key][column_names.index("DOMAIN")] = result["DOMAIN"]
        final_results[key][column_names.index("USUBJID")] = "%s.%s" % (result["STUDYID"], result["SUBJID"])
        final_results[key][column_names.index("SUBJID")] = result["SUBJID"]
        final_results[key][column_names.index("SITEID")] = result["SITEID"]
        final_results[key][column_names.index("INVID")] = result["INVID"]
        final_results[key][column_names.index("INVNAM")] = result["INVNAM"]
        final_results[key][column_names.index("COUNTRY")] = result["COUNTRY"]
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

    df = pd.DataFrame(columns=column_names)
    for subject, result in final_results.items():
      df.loc[len(df.index)] = result
    return df

  def variable_list(self, domain):
    results = []
    db = Neo4jConnection()
    with db.session() as session:
      query = """MATCH (sd:StudyDomainInstance)-[:HAS_VARIABLE]->(sv:StudyVariable) WHERE sd.name = "%s"
        RETURN sv.name as name ORDER BY toInteger(sv.ordinal)
      """ % (domain)
      result = session.run(query)
      for record in result:
        results.append(record["name"])
    return results

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