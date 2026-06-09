import logging
import traceback
from typing import List, Union, Literal
from .code import *
from .alias_code import *
#from .study_protocol_version import *
from .study_design import *
from d4kms_service import Neo4jConnection
from .base_node import NodeId
from .governance_date import GovernanceDate
from .study_amendment import StudyAmendment
from .study_identifier import StudyIdentifier
from .study_protocol_document_version import StudyProtocolDocumentVersion
from .study_title import StudyTitle
from .organization import Organization
from .study_design import StudyDesign

class StudyVersion(NodeId):
  versionIdentifier: str
  rationale: str
  studyType: Union[Code, None] = None
  studyPhase: Union[AliasCode, None] = None
  documentVersionId: Union[str, None] = None
  dateValues: List[GovernanceDate] = []
  amendments: List[StudyAmendment] = []
  businessTherapeuticAreas: List[Code] = []
  studyIdentifiers: List[StudyIdentifier] = []
  studyDesigns: List[StudyDesign] = []
  titles: List[StudyTitle] = []
  instanceType: Literal['StudyVersion']

  @classmethod
  def parent_properties(cls):
    return ["n.studyVersion", "n.studyRationale", "n.studyAcronym"]

  @classmethod
  def list(cls, uuid, page, size, filter):
    return cls.base_list("MATCH (m:Study {uuid: '%s'})-[]->(n:StudyVersion)" % (uuid), "ORDER BY n.versionIdentifier ASC", page, size, filter)

  def summary(self):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sv:StudyVersion {uuid: '%s'})
        WITH sv
        MATCH (sv)-[:TITLES_REL]->(st)-[:TYPE_REL]->(stc)
        MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si)-[:STUDY_IDENTIFIER_SCOPE_REL]->(o:Organization)-[:ORGANIZATION_TYPE_REL]->(oc:Code)
        MATCH (sv)-[:STUDY_PHASE_REL]->(ac:AliasCode)-[:STANDARD_CODE_REL]->(pc:Code)
        MATCH (sv)-[]->(sd:StudyDesign)
        RETURN sv, st, stc, si, pc, o, oc, sd ORDER BY sv.version
      """ % (self.uuid)
      print(f"QUERY: {query}")
      results = {}
      records = session.run(query)
      for record in records:
        sv = StudyVersion.as_dict(record['sv'])
        if sv['uuid'] not in results:
          result = sv
          result['titles'] = {}
          result['identifiers'] = {}
          result['study_designs'] = {}
          result['phase'] = ''
          results[sv['uuid']] = result
        else:
          result = results[sv['uuid']]
        stc = Code.as_dict(record['stc'])
        st = StudyTitle.as_dict(record['st'])
        si = StudyIdentifier.as_dict(record['si'])
        o = Organization.as_dict(record['o'])
        oc = Code.as_dict(record['oc'])
        sd = StudyDesign.as_dict(record['sd'])
        result['identifiers'][oc['decode']] = {'identifier': si['studyIdentifier'], 'organization': o['name']}
        #print(f"SI: {si}, {o}, {oc}")
        pc = Code.as_dict(record['pc'])
        result['titles'][stc['decode']] = st['text']
        result['phase'] = pc['decode']
        result['study_designs'][sd['uuid']] = sd
    print(f"RESULTS: {list(results.values())}")
    return list(results.values())[0]

  def study_name(self):
    db = Neo4jConnection()
    with db.session() as session:
      query = """MATCH (sv:StudyVersion {uuid: '%s'})<-[:VERSIONS_REL]-(s:Study) RETURN s.name as study_name""" % (self.uuid)
      # print(f"QUERY: {query}")
      records = session.run(query)
      results = [x for x in records.data()]
    db.close()
    return results[0]['study_name'] if results else None

  # @classmethod
  # def find_from_study_protocol_document_version(cls, uuid):
  #   db = Neo4jConnection()
  #   with db.session() as session:
  #     return session.execute_read(cls._find_from_spdv, uuid)

  # @staticmethod
  # def _find_from_spdv(tx, uuid):
  #   query = "MATCH (sv:StudyVersion)-[:DOCUMENT_VERSION_REL]->(spdv:StudyProtocolDocumentVersion {uuid: $uuid}) RETURN sv"
  #   result = tx.run(query, uuid=uuid)
  #   for row in result:
  #     return {'result': StudyVersion.wrap(row['sv'])}
  #   return {'error': f"Exception. Failed to find study version"}
  
  def protocol_document(self):
    try:
      db = Neo4jConnection()
      with db.session() as session:
        result = session.execute_read(self._protocol_document, self.uuid)
        if not result:
          return {'error': "Failed to find protocol version, operation failed"}
        return {'uuid': result.uuid}  
    except Exception as e:
      logging.error(f"Exception raised while finding protocol version")
      logging.error(f"Exception {e}\n{traceback.format_exc()}")
      return {'error': f"Exception. Failed to find protcol version"}

  @staticmethod
  def _protocol_document(tx, uuid):
    query = "MATCH (sv:StudyVersion {uuid: $uuid})-[:DOCUMENT_VERSION_REL]->(spdv:StudyProtocolDocumentVersion) RETURN spdv as protocol"
    result = tx.run(query, uuid=uuid)
    for row in result:
      return StudyProtocolDocumentVersion.wrap(row['protocol'])
    return None

  #   @classmethod
#   def exists(cls, identifier):
#     db = Neo4jConnection()
#     with db.session() as session:
#       result = session.execute_read(cls._exists, identifier)
#       return result

#   @classmethod
#   def delete(cls, uuid):
#     db = Neo4jConnection()
#     with db.session() as session:
#       session.execute_write(cls._delete_study, uuid)


#   def study_parameters(self):
#     db = Neo4jConnection()
#     with db.session() as session:
#       return session.execute_read(self._study_parameters, self.uuid)

#   def study_identifiers(self):
#     db = Neo4jConnection()
#     with db.session() as session:
#       return session.execute_read(self._study_identifiers, self.uuid)

#   def add_sponsor_identifier(self, identifier, name, scheme, scheme_identifier):
#     db = Neo4jConnection()
#     with db.session() as session:
#       return session.execute_write(self._study_sponsor_identifier, self.uuid, identifier, name, scheme, scheme_identifier)

#   def add_ct_dot_gov_identifier(self, identifier):
#     db = Neo4jConnection()
#     with db.session() as session:
#       return session.execute_write(self._study_ct_dot_gov_identifier, self.uuid, identifier)


#   @staticmethod
#   def _delete_study(tx, the_uuid):
#       query = """
#         MATCH (s:Study { uuid: $uuid1 })-[]->(sd:StudyDesign)
#         WITH s,sd
#         OPTIONAL MATCH (si:Site)-[:PARTICIPATES_IN]->(sd)
#         WITH s,sd,si
#         OPTIONAL MATCH (i:Investigator)-[:WORKS_AT]->(si)
#         WITH s,sd,si,i
#         OPTIONAL MATCH (subj:Subject)-[:AT_SITE]->(si)
#         WITH s,sd,si,i,subj
#         OPTIONAL MATCH (dp:DataPoint)-[:FOR_SUBJECT]->(subj)
#         DETACH DELETE si,i,subj,dp
#       """
#       result = tx.run(query, uuid1=the_uuid)
#       query = """
#         MATCH (s:Study { uuid: $uuid1 })-[ *1..]->(n)
#         DETACH DELETE n,s
#       """
#       result = tx.run(query, uuid1=the_uuid)

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

#     # def find_person(self, person_name):
#     #     with self.driver.session() as session:
#     #         result = session.read_transaction(self._find_and_return_person, person_name)
#     #         for row in result:
#     #             print("Found person: {row}".format(row=row))


#   @staticmethod
#   def _study_parameters(tx, uuid):
#     query = (
#       "MATCH (s:Study {uuid: $uuid}) "
#       "WITH s " 
#       "MATCH (s)-[:STUDY_TYPE]->(st:Code), "
#       "(s)-[:STUDY_PHASE]->(sp:Code)"
#       "RETURN st,sp"
#     )
#     result = tx.run(query, uuid=uuid)
#     for row in result:
#       return StudyParameters(**{ "studyType": Code.wrap(row['st']).__dict__, "studyPhase": Code.wrap(row['sp']).__dict__ })

#   @staticmethod
#   def _study_identifiers(tx, uuid):
#     results = []
#     query = (
#       "MATCH (s:Study {uuid: $uuid})-[:STUDY_IDENTIFIER]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE]->(o:Organisation)-[:ORGANISATION_TYPE]->(c:Code)"
#       "RETURN si,o,c"
#     )
#     result = tx.run(query, uuid=uuid)
#     for row in result:
#       identifier = StudyIdentifier.wrap(row['si'])
#       identifier.studyIdentifierScope = Organisation.wrap(row['o'])
#       identifier.studyIdentifierScope.organisationType = Code.wrap(row['c'])
#       results.append(identifier)
#     return results

#   @staticmethod
#   def _study_sponsor_identifier(tx, uuid, identifier, name, scheme, scheme_identifier):
#     results = []
#     query = """
#       MATCH (s:Study {uuid: $uuid1})
#       CREATE (s)-[:STUDY_IDENTIFIER]->(si:StudyIdentifier { uuid: $uuid2, studyIdentifier: $identifier })
#       CREATE (si)-[:STUDY_IDENTIFIER_SCOPE]->(o:Organisation {uuid: $uuid3,  organisationIdentifierScheme: $scheme, organisationIdentifier: $scheme_identifier, organisationName: $name })
#       CREATE (o)-[:ORGANISATION_TYPE]->(c:Code {uuid: $uuid4, code: 'C70793', codeSystem: 'CDISC', codeSystemVersion: '2022-03-25', decode: 'Clinical Study Sponsor'})
#       RETURN si.uuid as uuid
#     """
#     results = tx.run(query, uuid1=uuid, uuid2=str(uuid4()), uuid3=str(uuid4()), uuid4=str(uuid4()), identifier=identifier, name=name, scheme=scheme, scheme_identifier=scheme_identifier)
#     for row in results:
#       return row['uuid']
#     return None

#   @staticmethod
#   def _study_ct_dot_gov_identifier(tx, uuid, identifier):
#     results = []
#     query = """
#       MATCH (s:Study {uuid: $uuid1})
#       CREATE (s)-[:STUDY_IDENTIFIER]->(si:StudyIdentifier { uuid: $uuid2, studyIdentifier: $identifier })
#       CREATE (si)-[:STUDY_IDENTIFIER_SCOPE]->(o:Organisation { uuid: $uuid3, organisationIdentifierScheme: "USGOV", organisationIdentifier: "CT.gov", organisationName: "clinicaltrials.gov" })
#       CREATE (o)-[:ORGANISATION_TYPE]->(c:Code {uuid: $uuid4, code: 'C93453', codeSystem: 'CDISC', codeSystemVersion: '2022-03-25', decode: 'Study Registry'})
#       RETURN si.uuid as uuid
#     """
#     results = tx.run(query, uuid1=uuid, uuid2=str(uuid4()), uuid3=str(uuid4()), uuid4=str(uuid4()), identifier=identifier)
#     for row in results:
#       return row['uuid']
#     return None

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

  # @staticmethod
  # def _find_study(tx, the_uuid):
  #   query = (
  #     "MATCH (s:Study) "
  #     "WHERE s.uuid = $the_uuid "
  #     "RETURN s"
  #   )
  #   result = tx.run(query, the_uuid=the_uuid)
  #   return [row["s"] for row in result]
