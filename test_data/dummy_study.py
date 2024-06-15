import traceback
from uuid import uuid4
from d4kms_service import Neo4jConnection
from test_data.phase import Phase
from model.template.template_manager import TemplateManager

class DummyStudy():

  def __init__(self):
    self.phase = Phase()
    self.template_manager = TemplateManager()
    self.templates = self.template_manager.templates()

  def create(self, study):
    study['template_uuid'] = self.templates[0]['id']
    study['phase'] = self.phase.get(study['phase']) if 'phase' in study else self.phase.default()
    for item in ['brief_title', 'offical_title']:
      study[item] = study[item] if item in study else '<To Be Provided>'
    study['version'] = study['version'] if 'version' in study else 1
    try:
      db = Neo4jConnection()
      with db.session() as session:
        result = session.execute_write(self._create, study)
        if not result:
          return {'error': "Failed to create study, operation failed"}
        return {'result': result}
    except Exception as e:
      print(f"Exception raised while creating study")
      print(f"Exception {e}\n{traceback.format_exc()}")
      return {'error': f"Exception. Failed to create study"}

  @staticmethod
  def _create(tx, study):
    study_uuid = None
    if study['version'] == 1:      
      query = """
        CREATE (s:Study {id: $s_id, name: $s_name, description: $s_description, label: $s_label, uuid: $s_uuid1, instanceType: 'Study'}) RETURN s.uuid as uuid
      """
      result = tx.run(query, 
        s_id='STUDY_ROOT',
        s_name=study['name'], 
        s_description=study['description'], 
        s_label=study['label'],
        s_uuid1=str(uuid4())
      )
      for row in result:
        study_uuid = row['uuid']
    query = """
      MATCH (s:Study {name: $s_name})
      WITH s
      CREATE (sv:StudyVersion {id: $sv_id, name: $sv_name, description: $sv_description, label: $sv_label, rationale: $sv_rationale, versionIdentifier: $sv_version, 
        uuid: $sv_uuid, instanceType: 'StudyVersion'})
      CREATE (spd:StudyProtocolDocument {id: $spd_id, name: $spd_name, description: $spd_description, label: $spd_label, uuid: $spd_uuid, instanceType: 'StudyProtocolDocument'})
      CREATE (spdv:StudyProtocolDocumentVersion {id: $spdv_id, protocolVersion: $spdv_version, uuid: $spdv_uuid, instanceType: 'StudyProtocolDocumentVersion', templateUuid: $spdv_template})
      
      CREATE (si:StudyIdentifier {id: $si_id, studyIdentifier: 'ACME_001', uuid: $si_uuid, instanceType: 'StudyIdentifier'})
      CREATE (org:Organization {id: $org_id, identifierScheme: 'DUNS', identifier: '123456789', uuid: $org_uuid, instanceType: 'Organization'})
      
      CREATE (st1:StudyTitle {id: $st1_id, text: $st_brief_title, uuid: $st1_uuid, instanceType: 'StudyTitle'})
      CREATE (st2:StudyTitle {id: $st2_id, text: $st_official_title, uuid: $st2_uuid, instanceType: 'StudyTitle'})
      CREATE (st3:StudyTitle {id: $st3_id, text: $st_public_title, uuid: $st3_uuid, instanceType: 'StudyTitle'})
      CREATE (st4:StudyTitle {id: $st4_id, text: $st_scientific_title, uuid: $st4_uuid, instanceType: 'StudyTitle'})
      CREATE (st5:StudyTitle {id: $st5_id, text: $st_acronym, uuid: $st5_uuid, instanceType: 'StudyTitle'})

      CREATE (c1:Code {id: $c1_id, code: 'C85255', codeSystem: 'http://www.cdisc.org', codeSystemVersion: '2023-09-29', decode: 'Draft', uuid: $c1_uuid, instanceType: 'Code'})
      CREATE (c2:Code {id: $c2_id, code: 'C99905x1', codeSystem: 'http://www.cdisc.org', codeSystemVersion: '2023-09-29', decode: 'Brief Study Title', uuid: $c2_uuid, instanceType: 'Code'})
      CREATE (c3:Code {id: $c3_id, code: 'C99905x2', codeSystem: 'http://www.cdisc.org', codeSystemVersion: '2023-09-29', decode: 'Official Study Title', uuid: $c3_uuid, instanceType: 'Code'})
      CREATE (c4:Code {id: $c4_id, code: 'C99905x3', codeSystem: 'http://www.cdisc.org', codeSystemVersion: '2023-09-29', decode: 'Public Study Title', uuid: $c4_uuid, instanceType: 'Code'})
      CREATE (c5:Code {id: $c5_id, code: 'C99905x4', codeSystem: 'http://www.cdisc.org', codeSystemVersion: '2023-09-29', decode: 'Scientific Study Title', uuid: $c5_uuid, instanceType: 'Code'})
      CREATE (c6:Code {id: $c6_id, code: 'C94108', codeSystem: 'http://www.cdisc.org', codeSystemVersion: '2023-09-29', decode: 'Study Acronym', uuid: $c6_uuid, instanceType: 'Code'}) 
      CREATE (c7:Code {id: $c7_id, code: 'C70793', codeSystem: 'http://www.cdisc.org', codeSystemVersion: '2023-09-29', decode: 'Clinical Study Sponsor', uuid: $c7_uuid, instanceType: 'Code'})
      CREATE (c8:Code {id: $c8_id, code: $phase_c_code, codeSystem: 'http://www.cdisc.org', codeSystemVersion: '2023-09-29', decode: $phase_pt, uuid: $c8_uuid, instanceType: 'Code'})

      CREATE (ac1:AliasCode {id: $ac1_id, uuid: $ac1_uuid, instanceType: 'AliasCode'})
      
      CREATE (s)-[:VERSIONS_REL]->(sv)
      CREATE (s)-[:DOCUMENTED_BY_REL]->(spd)
      CREATE (spd)-[:VERSIONS_REL]->(spdv)
      CREATE (spdv)-[:PROTOCOL_STATUS_REL]->(c1)
      CREATE (sv)-[:DOCUMENT_VERSION_REL]->(spdv)
      CREATE (sv)-[:TITLES_REL]->(st1)-[:TYPE_REL]->(c2)
      CREATE (sv)-[:TITLES_REL]->(st2)-[:TYPE_REL]->(c3)
      CREATE (sv)-[:TITLES_REL]->(st3)-[:TYPE_REL]->(c4)
      CREATE (sv)-[:TITLES_REL]->(st4)-[:TYPE_REL]->(c5)
      CREATE (sv)-[:TITLES_REL]->(st5)-[:TYPE_REL]->(c6)
      CREATE (sv)-[:STUDY_IDENTIFIERS_REL]->(si)-[:STUDY_IDENTIFIER_SCOPE_REL]->(org)-[:ORGANIZATION_TYPE_REL]->(c7)
      CREATE (sv)-[:STUDY_PHASE_REL]->(ac1)-[:STANDARD_CODE_REL]->(c8)
      
      RETURN s.uuid as uuid
    """
    result = tx.run(query, 
      s_name=study['name'], 
      sv_id='STUDY_VERSION_1',
      sv_name=study['name'], 
      sv_description=study['description'], 
      sv_label=study['label'], 
      sv_rationale="<To Be provided>",
      sv_version=study['version'],
      spd_id='STUDY_PROTOCOL',
      spd_name="PROTOCOL", 
      spd_description="The protocol document for the study", 
      spd_label="Protocol document", 
      spdv_id='STUDY_PROTOCOL_1',
      spdv_version="0.1",
      spdv_template=study['template_uuid'],
      org_id='ORGANIZATION_1',
      si_id='STUDY_IDENTIFIER_1',
      st1_id="STUDY_TITLE_1",
      st2_id="STUDY_TITLE_2",
      st3_id="STUDY_TITLE_3",
      st4_id="STUDY_TITLE_4",
      st5_id="STUDY_TITLE_5",
      st_brief_title=study['brief_title'],
      st_official_title=study['offical_title'],
      st_public_title="<To Be provided>",
      st_scientific_title="<To Be provided>",
      st_acronym="<To Be provided>",
      c1_id='CODE_1',
      c2_id='CODE_2',
      c3_id='CODE_3',
      c4_id='CODE_4',
      c5_id='CODE_5',
      c6_id='CODE_6',
      c7_id='CODE_7',
      c8_id='CODE_8',
      ac1_id='ALIAS_CODE_1',
      sv_uuid=str(uuid4()),
      spd_uuid=str(uuid4()),
      spdv_uuid=str(uuid4()),
      org_uuid=str(uuid4()), 
      si_uuid=str(uuid4()), 
      c1_uuid=str(uuid4()), 
      c2_uuid=str(uuid4()), 
      c3_uuid=str(uuid4()), 
      c4_uuid=str(uuid4()), 
      c5_uuid=str(uuid4()), 
      c6_uuid=str(uuid4()), 
      c7_uuid=str(uuid4()), 
      c8_uuid=str(uuid4()), 
      ac1_uuid=str(uuid4()), 
      st1_uuid=str(uuid4()), 
      st2_uuid=str(uuid4()), 
      st3_uuid=str(uuid4()), 
      st4_uuid=str(uuid4()), 
      st5_uuid=str(uuid4()),
      phase_c_code=study['phase']['conceptId'],
      phase_pt=study['phase']['preferredTerm']
    )
    for row in result:
      return row['uuid']
