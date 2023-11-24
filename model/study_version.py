# from pydantic import BaseModel
from typing import List, Union
from model.code import *
from model.alias_code import *
from model.study_protocol_version import *
from model.study_design import *
from model.neo4j_connection import Neo4jConnection
from model.node import Node
from .governance_date import GovernanceDate
from .study_amendment import StudyAmendment
from .study_identifier import StudyIdentifier
from .study_protocol_document_version import StudyProtocolDocumentVersion

# from service.ra_service import RAService
# from datetime import datetime
# from utility.utility import *
# from uuid import UUID, uuid4

class StudyVersion(NodeId):
  studyTitle: str
  versionIdentifier: str
  rationale: str
  studyAcronym: str
  type: Union[Code, None] = None
  studyPhase: Union[AliasCode, None] = None
  documentVersion: Union[StudyProtocolDocumentVersion, None] = None
  dateValues: List[GovernanceDate] = []
  amendments: List[StudyAmendment] = []
  businessTherapeuticAreas: List[Code] = []
  studyIdentifiers: List[StudyIdentifier] = []
  studyDesigns: List[StudyDesign] = []

  @classmethod
  def parent_properties(cls):
    return ["n.studyTitle", "n.studyVersion", "n.studyRationale", "n.studyAcronym"]

  @classmethod
  def list(cls, uuid, page, size, filter):
    return cls.base_list("MATCH (m:Study {uuid: '%s'})-[]->(n:StudyVersion)" % (uuid), "ORDER BY n.studyTitle ASC", page, size, filter)

  def study_designs(self):
    db = Neo4jConnection()
    with db.session() as session:
      return session.execute_read(self._study_designs, self.uuid)

  @staticmethod
  def _study_designs(tx, uuid):
    results = []
    query = "MATCH (s:StudyVersion {uuid: $uuid})-[:STUDY_DESIGNS]->(sd:StudyDesign) RETURN sd"
    result = tx.run(query, uuid=uuid)
    for row in result:
      print(f"ROW: {row}")
      results.append(StudyDesign.wrap(row['sd']))
    return results

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
#   def _create_study(tx, identifier, title):
#     ra_service = RAService()
#     ns = ra_service.namespace_by_name("d4k Study namespace")
#     ra = ra_service.registration_authority_by_namespace_uuid(ns['uuid'])
#     sv = SemanticVersion()
#     sv.draft()
#     study_uri = extend_uri(ns['value'], "dataset/%s" % (format_name(identifier)))
#     study_design_uri = extend_uri(study_uri, "sd-1")
#     query = (
#       "CREATE (s:Study { studyTitle: $title, uuid: $uuid1, uri: $uri1 })"
#       "CREATE (si:ScopedIdentifier { identifier: $id, version: 1, semantic_version: $sv, uuid: $uuid2 })"
#       "CREATE (ns:Namespace { uuid: $uuid8, namespace_uri: $ns_uri, namespace_value: $ns_value })"
#       "CREATE (rs:RegistrationStatus { effective_date: $date, registration_status: 'Draft', until_date: '', uuid: $uuid9 })"
#       "CREATE (ra:RegistrationAuthority { uuid: $uuid10, registration_authority_uri: $ra_uri, owner: $ra_owner })"
#       "CREATE (sd:StudyDesign { uuid: $uuid3, uri: $uri2 })"
#       "CREATE (sc:StudyCell { uuid: $uuid4 })"
#       "CREATE (sa:StudyArm { uuid: $uuid5, studyArmName: 'Single Arm', studyArmDesc: 'A single arm for this study'  })"
#       "CREATE (se:StudyEpoch { uuid: $uuid6, studyEpochName: 'Single Epoch', studyEpochDesc: 'A single epoch for this study' })"
#       "CREATE (wf:Workflow { uuid: $uuid7, workflowName: 'SoA', workflowDesc: 'The SoA workflow', uniqueLabel: 'wf-1' })"
#       "CREATE (st:Code {uuid: $uuid11, code: 'C12345', codeSystem: 'CDISC', codeSystemVersion: '1', decode: 'Observational'})"
#       "CREATE (sp:Code {uuid: $uuid12, code: 'C12346', codeSystem: 'CDISC', codeSystemVersion: '1', decode: 'None'})"
#       "CREATE (s)-[:IDENTIFIED_BY]->(si)"
#       "CREATE (s)-[:STUDY_TYPE]->(st)"
#       "CREATE (s)-[:STUDY_PHASE]->(sp)"
#       "CREATE (si)-[:SCOPED_BY]->(ns)"
#       "CREATE (s)-[:HAS_STATUS]->(rs)"
#       "CREATE (rs)-[:MANAGED_BY]->(ra)"
#       "CREATE (s)-[:STUDY_DESIGN]->(sd)"
#       "CREATE (sd)-[:STUDY_CELL]->(sc)"
#       "CREATE (sd)-[:STUDY_WORKFLOW]->(wf)"
#       "CREATE (sc)-[:STUDY_ARM]->(sa)"
#       "CREATE (sc)-[:STUDY_EPOCH]->(se)"
#       "RETURN s.uuid as uuid"
#     )
#     result = tx.run(query, 
#       title=title, 
#       id=identifier, 
#       sv=str(sv),
#       ns_uri=ns['uri'],
#       ns_value=ns['value'],
#       ra_uri=ra['uri'],
#       ra_owner=ra['name'],
#       date=datetime.now().strftime("%Y-%m-%d"),
#       uri1=study_uri,
#       uri2=study_design_uri,
#       uuid1=str(uuid4()), 
#       uuid2=str(uuid4()),
#       uuid3=str(uuid4()),
#       uuid4=str(uuid4()),
#       uuid5=str(uuid4()),
#       uuid6=str(uuid4()),
#       uuid7=str(uuid4()),
#       uuid8=str(uuid4()),
#       uuid9=str(uuid4()),
#       uuid10=str(uuid4()),
#       uuid11=str(uuid4()),
#       uuid12=str(uuid4())
#     )
# #      try:
#     for row in result:
#       return row["uuid"]
#     return None
# #      except ServiceUnavailable as exception:
# #        logging.error("{query} raised an error: \n {exception}".format(
# #          query=query, exception=exception))
# #        raise

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
