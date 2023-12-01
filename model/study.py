from .node import NodeNameLabelDesc
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
        if result:
          return {'uuid': result}
        else:
          return {'error': "Failed to create study, operation failed"}
    except Exception as e:
      return {'error': f"Exception '{e}'. Failed to create study"}


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

