from .node import NodeNameLabelDesc
from .study_protocol_document_version import StudyProtocolDocumentVersion
from .neo4j_connection import Neo4jConnection
from uuid import UUID, uuid4

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
      return {'error': f"Exception '{e}'. Failed to create study"}

  def protocol(self):
    try:
      db = Neo4jConnection()
      with db.session() as session:
        result = session.execute_read(self._protocol_exists, self.uuid)
        if not result:
          result = session.execute_write(self._create_protocol, self.uuid)
          if not result:
            return {'error': "Failed to create protocol, operation failed"}
        return {'uuid': result}  
    except Exception as e:
      return {'error': f"Exception '{e}'. Failed to find or create protocol document"}

  @staticmethod
  def _create_study(tx, name, description, label):
    query = """
      CREATE (s:Study {id: $s_id, name: $s_name, description: $s_description, label: $s_label, uuid: $uuid1})
      CREATE (sv:StudyVersion {name: $sv_name, description: $sv_description, label: $sv_label, rationale: $sv_rationale, 
        studyAcronym:	$sv_acronym, studyTitle: $sv_title, versionIdentifier: $sv_version, uuid: $uuid2})
      CREATE (s)-[:VERSIONS_REL]->(sv)
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
      uuid1=str(uuid4()), 
      uuid2=str(uuid4())
    )
    for row in result:
      return row["uuid"]
    return None

  @staticmethod
  def _create_protocol(tx, uuid):
    query = """
      MATCH (s:Study {uuid: $uuid})
      WITH s
      CREATE (spd:StudyProtocolDocument {id: $spd_id, name: $spd_name, description: $spd_description, label: $spd_label, uuid: $uuid1})
      CREATE (spdv:StudyProtocolDocumentVersion {id: $spdv_id, name: $spdv_name, description: $spdv_description, label: $spdv_label, 
        protocolVersion: $sv_version, briefTitle: $brief_title, officialTitle: $official_title=, $publicTitle=public_title,
        scientificTitle: $scientific_title, uuid: $uuid2})
      CREATE (s)-DOCUMENTED_BY_REL->(spd)
      CREATE (spd)-[:VERSIONS_REL]->(spdv)
      RETURN spdv.uuid as uuid
    """
    result = tx.run(query, 
      spd_id='STUDY_PROTOCOL',
      spd_name="PROTOCOL", 
      spd_description="The protocol document for the study", 
      spd_label="Protocol document", 
      spdv_id='STUDY_PROTOCOL_1',
      spdv_name="PROTOCOL_1", 
      spdv_description="The protocol document for the study", 
      spdv_label="Protocol document", 
      spdv_version="0.1",
      briefTitle="<To Be provided>",
      officialTitle="<To Be provided>",
      publicTitle="<To Be provided>",
      scientificTitle="<To Be provided>",
      uuid1=str(uuid4()), 
      uuid2=str(uuid4())
    )
    for row in result:
      return row['uuid']
    return None

  @staticmethod
  def _protocol_exists(tx, uuid):
    query = """
      MATCH (s:Study {uuid: $uuid})-[:DOCUMENTED_BY_REL]->(pd:StudyProtocolDocument)-[:VERSIONS_REL]->(pdv:StudyProtocolDocumentVersion)
      RETURN pdv.uuid as uuid
    """
    result = tx.run(query, uuid=uuid)
    if result.peek():
      for row in result:
        return row['uuid']
    return None      

  # @classmethod
  # def delete(cls, uuid):
  #   db = Neo4jConnection()
  #   with db.session() as session:
  #     session.execute_write(cls._delete_study, uuid)

  # @staticmethod
  # def _delete_study(tx, the_uuid):
  #     query = """
  #       MATCH (s:Study { uuid: $uuid1 })-[]->(sd:StudyDesign)
  #       WITH s,sd
  #       OPTIONAL MATCH (si:Site)-[:PARTICIPATES_IN]->(sd)
  #       WITH s,sd,si
  #       OPTIONAL MATCH (i:Investigator)-[:WORKS_AT]->(si)
  #       WITH s,sd,si,i
  #       OPTIONAL MATCH (subj:Subject)-[:AT_SITE]->(si)
  #       WITH s,sd,si,i,subj
  #       OPTIONAL MATCH (dp:DataPoint)-[:FOR_SUBJECT]->(subj)
  #       DETACH DELETE si,i,subj,dp
  #     """
  #     result = tx.run(query, uuid1=the_uuid)
  #     query = """
  #       MATCH (s:Study { uuid: $uuid1 })-[ *1..]->(n)
  #       DETACH DELETE n,s
  #     """
  #     result = tx.run(query, uuid1=the_uuid)

#   @staticmethod
#   def _exists(tx, identifier):
#       query = (
#         "MATCH (s:Study)-[:IDENTIFIED_BY]->(si:ScopedIdentifier { identifier: $id } )"
#         "RETURN s.uuid as uuid"
#       )
#       result = tx.run(query, id=identifier)
#       if result.peek() == None:
#         return False
#       else:
#         return True
# #      try:

    # def find_person(self, person_name):
    #     with self.driver.session() as session:
    #         result = session.read_transaction(self._find_and_return_person, person_name)
    #         for row in result:
    #             print("Found person: {row}".format(row=row))

  # @staticmethod
  # def _list(tx):
  #   results = []
  #   query = (
  #     "MATCH (s:Study)"
  #     "RETURN s.uuid as uuid"
  #   )
  #   result = tx.run(query)
  #   for row in result:
  #     results.append(row['uuid'])
  #   return results

