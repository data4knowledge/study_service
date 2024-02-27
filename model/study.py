import logging
import traceback
from typing import List, Literal, Union
from .base_node import NodeNameLabelDesc
from .study_version import StudyVersion
from .study_protocol_document import StudyProtocolDocument
from d4kms_service import Neo4jConnection
from uuid import uuid4

class Study(NodeNameLabelDesc):
  versions: List[StudyVersion] = []
  documentedBy: Union[StudyProtocolDocument, None] = None
  instanceType: Literal['Study']

  @classmethod
  def list(cls, page, size, filter):
    return cls.base_list("MATCH (n:Study)", "ORDER BY n.name ASC", page, size, filter)

  @classmethod
  def create(cls, name, description, label, template_uuid):
    try:
      db = Neo4jConnection()
      with db.session() as session:
        result = session.execute_write(cls._create_study, name, description, label, template_uuid)
        if not result:
          return {'error': "Failed to create study, operation failed"}
        return {'uuid': result}  
    except Exception as e:
      logging.error(f"Exception raised while creating study")
      logging.error(f"Exception {e}\n{traceback.format_exc()}")
      return {'error': f"Exception. Failed to create study"}

  @staticmethod
  def _create_study(tx, name, description, label, template_uuid):
    query = """
      CREATE (s:Study {id: $s_id, name: $s_name, description: $s_description, label: $s_label, uuid: $s_uuid1, instanceType: 'Study'})
      CREATE (sv:StudyVersion {id: $sv_id, name: $sv_name, description: $sv_description, label: $sv_label, rationale: $sv_rationale, versionIdentifier: $sv_version, 
        uuid: $sv_uuid, instanceType: 'StudyVersion'})
      CREATE (spd:StudyProtocolDocument {id: $spd_id, name: $spd_name, description: $spd_description, label: $spd_label, uuid: $spd_uuid, instanceType: 'StudyProtocolDocument'})
      CREATE (spdv:StudyProtocolDocumentVersion {id: $spdv_id, protocolVersion: $spdv_version, uuid: $spdv_uuid, instanceType: 'StudyProtocolDocumentVersion', templateUuid: $spdv_template})
      
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
      RETURN s.uuid as uuid
    """
    result = tx.run(query, 
      s_id='STUDY_ROOT',
      s_name=name, 
      s_description=description, 
      s_label=label, 
      sv_id='STUDY_VERSION_1',
      sv_name=name, 
      sv_description=description, 
      sv_label=label, 
      sv_rationale="<To Be provided>",
      sv_version="0.1",
      spd_id='STUDY_PROTOCOL',
      spd_name="PROTOCOL", 
      spd_description="The protocol document for the study", 
      spd_label="Protocol document", 
      spdv_id='STUDY_PROTOCOL_1',
      spdv_version="0.1",
      spdv_template=template_uuid,
      st1_id="STUDY_TITLE_1",
      st2_id="STUDY_TITLE_2",
      st3_id="STUDY_TITLE_3",
      st4_id="STUDY_TITLE_4",
      st5_id="STUDY_TITLE_5",
      st_brief_title="<To Be provided>",
      st_official_title="<To Be provided>",
      st_public_title="<To Be provided>",
      st_scientific_title="<To Be provided>",
      st_acronym="<To Be provided>",
      c1_id='CODE_1',
      c2_id='CODE_2',
      c3_id='CODE_3',
      c4_id='CODE_4',
      c5_id='CODE_5',
      c6_id='CODE_6',
      s_uuid1=str(uuid4()), 
      sv_uuid=str(uuid4()),
      spd_uuid=str(uuid4()), 
      c1_uuid=str(uuid4()), 
      c2_uuid=str(uuid4()), 
      c3_uuid=str(uuid4()), 
      c4_uuid=str(uuid4()), 
      c5_uuid=str(uuid4()), 
      c6_uuid=str(uuid4()), 
      st1_uuid=str(uuid4()), 
      st2_uuid=str(uuid4()), 
      st3_uuid=str(uuid4()), 
      st4_uuid=str(uuid4()), 
      st5_uuid=str(uuid4()), 
      spdv_uuid=str(uuid4())
    )
    for row in result:
      return row["uuid"]
    return None

 