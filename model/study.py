import logging
import traceback
from .node import NodeNameLabelDesc
from .neo4j_connection import Neo4jConnection
from uuid import uuid4

class Study(NodeNameLabelDesc):
  
  @classmethod
  def list(cls, page, size, filter):
    return cls.base_list("MATCH (n:Study)", "ORDER BY n.name ASC", page, size, filter)

  @classmethod
  def create(cls, name, description, label):
    try:
      db = Neo4jConnection()
      with db.session() as session:
        result = session.execute_write(cls._create_study, name, description, label)
        if not result:
          return {'error': "Failed to create study, operation failed"}
        return {'uuid': result}  
    except Exception as e:
      logging.error(f"Exception raised while creating study")
      logging.error(f"Exception {e}\n{traceback.format_exc()}")
      return {'error': f"Exception. Failed to create study"}

  @staticmethod
  def _create_study(tx, name, description, label):
    query = """
      CREATE (s:Study {id: $s_id, name: $s_name, description: $s_description, label: $s_label, uuid: $s_uuid1})
      CREATE (sv:StudyVersion {id: $sv_id, name: $sv_name, description: $sv_description, label: $sv_label, rationale: $sv_rationale, 
        studyAcronym:	$sv_acronym, studyTitle: $sv_title, versionIdentifier: $sv_version, uuid: $sv_uuid})
      CREATE (spd:StudyProtocolDocument {id: $spd_id, name: $spd_name, description: $spd_description, label: $spd_label, uuid: $spd_uuid})
      CREATE (spdv:StudyProtocolDocumentVersion {id: $spdv_id, name: $spdv_name, description: $spdv_description, label: $spdv_label, 
        protocolVersion: $spdv_version, briefTitle: $spdv_brief_title, officialTitle: $spdv_official_title, publicTitle: $spdv_public_title,
        scientificTitle: $spdv_scientific_title, uuid: $spdv_uuid})
      CREATE (s)-[:VERSIONS_REL]->(sv)
      CREATE (c:Code {id: $c_id, code: 'C85255', codeSystem: 'http://www.cdisc.org', codeSystemVersion: '2023-09-29', decode: 'Draft', uuid: $c_uuid})
      CREATE (s)-[:DOCUMENTED_BY_REL]->(spd)
      CREATE (spd)-[:VERSIONS_REL]->(spdv)
      CREATE (spdv)-[:PROTOCOL_STATUS_REL]->(c)
      CREATE (sv)-[:DOCUMENT_VERSION_REL]->(spdv)
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
      sv_acronym="<To Be provided>",
      sv_title="<To Be provided>",
      sv_version="0.1",
      spd_id='STUDY_PROTOCOL',
      spd_name="PROTOCOL", 
      spd_description="The protocol document for the study", 
      spd_label="Protocol document", 
      spdv_id='STUDY_PROTOCOL_1',
      spdv_name="PROTOCOL_1", 
      spdv_description="The protocol document for the study", 
      spdv_label="Protocol document", 
      spdv_version="0.1",
      spdv_brief_title="<To Be provided>",
      spdv_official_title="<To Be provided>",
      spdv_public_title="<To Be provided>",
      spdv_scientific_title="<To Be provided>",
      c_id='STUDY_PROTOCOL_STATUS_1',
      s_uuid1=str(uuid4()), 
      sv_uuid=str(uuid4()),
      spd_uuid=str(uuid4()), 
      c_uuid=str(uuid4()), 
      spdv_uuid=str(uuid4())
    )
    for row in result:
      return row["uuid"]
    return None

 