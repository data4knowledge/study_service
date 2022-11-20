from uuid import UUID
from typing import Union
from model.node import Node
from model.neo4j_connection import Neo4jConnection

class StudyDomainInstance(Node):
  uuid: Union[UUID, None] = None
  name: str

  def data(self):
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      # TODO Add in enabled true for USING BC relationship
      #  WITH DISTINCT bc, bcr1, sd, sv, fdt, bdt, sdp, wfi
      #  OPTIONAL MATCH (sv)-[:BC_RESTRICTION]->(bcr2:BiomedicalConceptRef) WHERE IS NULL bcr2 OR bcr1.uuid = bcr2.uuid
      # si.org_code as study_id,
      query = """
        MATCH (std:StudyDesign)-[]->(sd:StudyDomainInstance {name: '%s'})
        WITH std,sd
        MATCH (wfi:WorkflowItem)-[:HAS_STUDY_BC_INSTANCE]->(bc:StudyBCInstance)<-[:BC_REF]-(bcr1:BiomedicalConceptRef)<-[:USING_BC]-(sd)-[:HAS_VARIABLE]->(sv:StudyVariable)
          -[:CLINICAL_RECORDING_REF]->(fdt:ClinicalRecordingRef)<-[:CLINICAL_RECORDING_REF]-(bdt:StudyBCDataTypeProperty)
          <-[:FOR_DATA_POINT]-(sdp:DataPoint)
        WITH DISTINCT std, sd, bc, sv, fdt, bdt, sdp, wfi 
        MATCH (wfi)-[:WORKFLOW_ITEM_ENCOUNTER]->(v:Encounter)<-[]-(e:StudyEpoch)
        WITH DISTINCT std, sd, bc, sv, fdt, bdt, sdp, wfi, v, e
        MATCH (sdp)-[:FOR_SUBJECT]->(subj:Subject)-[:PARTICIPATES_IN]->(std)-[]->(sd)
        WITH DISTINCT std, sd, bc, sv, fdt, bdt, sdp, wfi, v, e, subj
        MATCH (subj)-[:AT_SITE]->(site:Site)
        WITH DISTINCT std, sd, bc, sv, fdt, bdt, sdp, wfi, v, e, subj, site
        MATCH (ct:ValueSet)<-[:HAS_RESPONSE]-()<-[:HAS_STUDY_BC_DATA_TYPE]-(StudyBCItem {name: "Test"})<-[:HAS_STUDY_BC_ITEM]-(bc)-[*]->(bdt)
        RETURN DISTINCT sd.name as domain, sv.name as variable, sdp.value as data, wfi.id as uuid, v.name as visit, e.study_epoch_name as epoch, subj.identifier as subject, ct.notation as test_code, 
          site.site_id as siteid, site.inv_name as invnam, site.inv_id as invid, site.country_code as country
      """ % (self.name)
      print(query)
      rows = session.run(query)
      for row in rows:
        results.append(row)
        print ("%s, %s, %s, %s, %s, %s, [%s -> %s]" % (row["domain"], row["variable"], row["test_code"], row["subject"], row["uuid"], row["data"], row["visit"], row["epoch"]))
      return results