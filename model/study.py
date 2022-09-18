from pydantic import BaseModel
from typing import List, Union
from model.code import *
from model.study_identifier import *
from model.scoped_identifier import *
from model.registration_status import *
from model.study_protocol_version import *
from model.study_design import *
from model.neo4j_connection import Neo4jConnection
from model.semantic_version import SemanticVersion
from model.node import Node
from service.ra_service import RAService
from datetime import datetime

from uuid import UUID, uuid4

class StudyIn(BaseModel):
  title: str
  identifier: str

class StudyOut(BaseModel):
  uuid = str
  #uri = str
  studyTitle: str
  studyVersion: str
  studyType: dict
  studyPhase: dict
  studyIdentifiers: dict
  studyProtocolVersions: dict
  studyDesigns: dict

class StudyParameters(BaseModel):
  studyType: dict
  studyPhase: dict

class StudySummary(BaseModel):
  #uri: str
  uuid: str
  studyTitle: str
  identified_by: dict
  has_status: dict

class StudyList(BaseModel):
  items: List[StudySummary]
  page: int
  size: int
  filter: str
  count: int

  @classmethod
  def list(cls, page, size, filter):
    if filter == "":
      count = Study.full_count()
    else:
      count = Study.filter_count(filter)
    results = {'items': [], 'page': page, 'size': size, 'filter': filter, 'count': count }
    results['items'] = Study.list(page, size, filter)
    return results

class Study(Node):
  uuid: str
  uri: str = ""
  studyTitle: str
  studyVersion: str = ""
  studyType: Union[Code, UUID, None]
  studyPhase: Union[Code, UUID, None]
  studyIdentifiers: Union[List[StudyIdentifier], List[UUID], None]
  studyProtocolVersions: Union[List[StudyProtocolVersion], List[UUID], None]
  studyDesigns: Union[List[StudyDesign], List[UUID], None]
  identified_by: Union[ScopedIdentifier, UUID, None] #EXTENSION
  has_status: Union[RegistrationStatus, UUID, None] # EXTENSION

  @classmethod
  def find_full(cls, uuid):
    db = Neo4jConnection()
    with db.session() as session:
      return session.execute_read(cls._find_full, uuid)

  @staticmethod
  def _find_full(tx, uuid):
    study = None
    query = """
      MATCH (s:Study { uuid: '%s' })-[:IDENTIFIED_BY]->(si:ScopedIdentifier)-[:SCOPED_BY]->(ns:Namespace),
      (s)-[:HAS_STATUS]->(rs:RegistrationStatus)-[:MANAGED_BY]->(ra:RegistrationAuthority)
      RETURN s,si,ns,rs,ra
    """ % (uuid)
    result = tx.run(query, uuid1=uuid)
    for row in result:
      if study == None:
        study = Study.wrap(row['s'])
        study.studyDesigns = []
        study.identified_by = ScopedIdentifier.wrap(row['si'])
        study.identified_by.scoped_by = Namespace.wrap(row['ns'])
        study.has_status = RegistrationStatus.wrap(row['si'])
        study.has_status.managed_by = RegistrationAuthority.wrap(row['ra'])
    return study

  @classmethod
  def create(cls, identifier, title):
    db = Neo4jConnection()
    with db.session() as session:
      if not session.execute_read(cls._exists, identifier):
        result = session.execute_write(cls._create_study, identifier, title)
        return result
      else:
        return None

  @classmethod
  def list(cls, page=0, size=0, filter=""):
    ra = {}
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      skip_offset_clause = ""
      if page != 0:
        offset = (page - 1) * size
        skip_offset_clause = "SKIP %s LIMIT %s" % (offset, size)
      if filter == "":
        query = """
          MATCH (n:Study)-[:IDENTIFIED_BY]->(ni:ScopedIdentifier),
          (n)-[:HAS_STATUS]->(ns:RegistrationStatus)-[:MANAGED_BY]->(ra:RegistrationAuthority) 
          RETURN n,ni,ns,ra ORDER BY toInteger(ni.version) DESC %s
        """ % (skip_offset_clause)
      else:
        identifier_filter_clause = cls.build_filter_clause(filter, cls.IDENTIFIER_PROPERTIES)
        status_filter_clause = cls.build_filter_clause(filter, cls.STATUS_PROPERTIES)
        parent_filter_clause = cls.build_filter_clause(filter, cls.PARENT_PROPERTIES)
        query = """
          CALL {
              MATCH (n:Study)-[:IDENTIFIED_BY]->(ni:ScopedIdentifier) %s
              RETURN n
            UNION
              MATCH(n:Study)-[:HAS_STATUS]->(ns:RegistrationStatus)-[:MANAGED_BY]->(ra:RegistrationAuthority) %s
              RETURN n
            UNION
              MATCH(n:Study) %s
              RETURN n
          }
          WITH n 
          MATCH (n)-[:IDENTIFIED_BY]->(ni:ScopedIdentifier),
          (n)-[:HAS_STATUS]->(ns:RegistrationStatus)-[:MANAGED_BY]->(ra:RegistrationAuthority) 
          RETURN n,ni,ns,ra ORDER BY toInteger(ni.version) DESC %s
        """ % (identifier_filter_clause, status_filter_clause, parent_filter_clause, skip_offset_clause)
      query_results = session.run(query)
      for query_result in query_results:
        rel = dict(query_result['n'])
        rel['identified_by'] = dict(query_result['ni'])
        rel['has_status'] = dict(query_result['ns'])
        rel['has_status']['managed_by'] = dict(query_result['ra'])
        results.append(rel)
      return results

  @classmethod
  def full_count(cls):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (n:Study)-[:IDENTIFIED_BY]->(ni:ScopedIdentifier)
          RETURN COUNT(n) as count 
      """
      query_results = session.run(query)
      for result in query_results:
        return result['count']
      return 0

  @classmethod
  def filter_count(cls, filter):
    db = Neo4jConnection()
    with db.session() as session:
      identifier_filter_clause = cls.build_filter_clause(filter, cls.IDENTIFIER_PROPERTIES)
      status_filter_clause = cls.build_filter_clause(filter, cls.STATUS_PROPERTIES)
      parent_filter_clause = cls.build_filter_clause(filter, cls.PARENT_PROPERTIES)
      query = """
          MATCH (n:Study)-[:IDENTIFIED_BY]->(ni:ScopedIdentifier) %s
          RETURN n
        UNION
          MATCH(n:Study)-[:HAS_STATUS]->(ns:RegistrationStatus)-[:MANAGED_BY]->(ra:RegistrationAuthority) %s
          RETURN n
        UNION
          MATCH(n:Study) %s
          RETURN n
      """ % (identifier_filter_clause, status_filter_clause, parent_filter_clause)
      query_results = session.run(query)
      return len(query_results)

  @classmethod
  def build_filter_clause(cls, filter, properties):
    filter_clause_parts = []
    for property in properties:
      filter_clause_parts.append("toLower(%s) CONTAINS toLower('%s')" % (property, filter))
    filter_clause = " OR ".join(filter_clause_parts)
    filter_clause = "WHERE %s " % (filter_clause)
    return filter_clause

  @classmethod
  def exists(cls, identifier):
    db = Neo4jConnection()
    with db.session() as session:
      result = session.execute_read(cls._exists, identifier)
      return result

  @classmethod
  def delete(cls, uuid):
    db = Neo4jConnection()
    with db.session() as session:
      session.execute_write(cls._delete_study, uuid)

  def study_designs(self):
    db = Neo4jConnection()
    with db.session() as session:
      return session.execute_read(self._study_designs, self.uuid)

  def study_parameters(self):
    db = Neo4jConnection()
    with db.session() as session:
      return session.execute_read(self._study_parameters, self.uuid)

  def study_identifiers(self):
    db = Neo4jConnection()
    with db.session() as session:
      return session.execute_read(self._study_identifiers, self.uuid)

  @staticmethod
  def _create_study(tx, identifier, title):
    ra_service = RAService()
    ns = ra_service.namespace_by_name("d4k Study namespace")
    ra = ra_service.registration_authority_by_namespace_uuid(ns['uuid'])
    sv = SemanticVersion()
    sv.draft()
    query = (
      "CREATE (s:Study { studyTitle: $title, uuid: $uuid1 })"
      "CREATE (si:ScopedIdentifier { identifier: $id, version: 1, semantic_version: $sv, uuid: $uuid2 })"
      "CREATE (ns:Namespace { uuid: $uuid8, namespace_uri: $ns_uri, namespace_value: $ns_value })"
      "CREATE (rs:RegistrationStatus { effective_date: $date, registration_status: 'Draft', until_date: '', uuid: $uuid9 })"
      "CREATE (ra:RegistrationAuthority { uuid: $uuid10, registration_authority_uri: $ra_uri, owner: $ra_owner })"
      "CREATE (sd:StudyDesign { uuid: $uuid3 })"
      "CREATE (sc:StudyCell { uuid: $uuid4 })"
      "CREATE (sa:StudyArm { uuid: $uuid5 })"
      "CREATE (se:StudyEpoch { uuid: $uuid6, studyEpochName: 'Single Epoch', studyEpochDesc: 'A single epoch for this study' })"
      "CREATE (wf:Workflow { uuid: $uuid7, workflowName: 'SoA', workflowDesc: 'The SoA workflow' })"
      "CREATE (st:Code {uuid: $uuid11, code: 'C12345', codeSystem: 'CDISC', codeSystemVersion: '1', decode: 'Observational'})"
      "CREATE (sp:Code {uuid: $uuid12, code: 'C12346', codeSystem: 'CDISC', codeSystemVersion: '1', decode: 'None'})"
      "CREATE (s)-[:IDENTIFIED_BY]->(si)"
      "CREATE (s)-[:STUDY_TYPE]->(st)"
      "CREATE (s)-[:STUDY_PHASE]->(sp)"
      "CREATE (si)-[:SCOPED_BY]->(ns)"
      "CREATE (s)-[:HAS_STATUS]->(rs)"
      "CREATE (rs)-[:MANAGED_BY]->(ra)"
      "CREATE (s)-[:STUDY_DESIGN]->(sd)"
      "CREATE (sd)-[:STUDY_CELL]->(sc)"
      "CREATE (sd)-[:STUDY_WORKFLOW]->(wf)"
      "CREATE (sc)-[:STUDY_ARM]->(sa)"
      "CREATE (sc)-[:STUDY_EPOCH]->(se)"
      "RETURN s.uuid as uuid"
    )
    result = tx.run(query, 
      title=title, 
      id=identifier, 
      sv=str(sv),
      ns_uri=ns['uri'],
      ns_value=ns['value'],
      ra_uri=ra['uri'],
      ra_owner=ra['name'],
      date=datetime.now().strftime("%Y-%m-%d"),
      uuid1=str(uuid4()), 
      uuid2=str(uuid4()),
      uuid3=str(uuid4()),
      uuid4=str(uuid4()),
      uuid5=str(uuid4()),
      uuid6=str(uuid4()),
      uuid7=str(uuid4()),
      uuid8=str(uuid4()),
      uuid9=str(uuid4()),
      uuid10=str(uuid4()),
      uuid11=str(uuid4()),
      uuid12=str(uuid4())
    )
#      try:
    for row in result:
      return row["uuid"]
    return None
#      except ServiceUnavailable as exception:
#        logging.error("{query} raised an error: \n {exception}".format(
#          query=query, exception=exception))
#        raise

  @staticmethod
  def _delete_study(tx, the_uuid):
      query = """
        MATCH (s:Study { uuid: $uuid1 })
          -[:STUDY_DESIGN|IDENTIFIED_BY|SCOPED_BY|HAS_STATUS|MANAGED_BY|STUDY_CELL|STUDY_ARM|STUDY_EPOCH|
          STUDY_WORKFLOW|STUDY_DATA_COLLECTION|STUDY_TYPE|STUDY_PHASE|
          STUDY_ACTIVITY|STUDY_ENCOUNTER|WORKFLOW_ITEM|WORKFLOW_ITEM_ENCOUNTER|WORKFLOW_ITEM_ACTIVITY  *1..]->(n)
        DETACH DELETE (n)
        DETACH DELETE (s)
      """
#      try:
      result = tx.run(query, uuid1=the_uuid)
#      except ServiceUnavailable as exception:
#        logging.error("{query} raised an error: \n {exception}".format(
#          query=query, exception=exception))
#        raise

  @staticmethod
  def _exists(tx, identifier):
      query = (
        "MATCH (s:Study)-[:IDENTIFIED_BY]->(si:ScopedIdentifier { identifier: $id } )"
        "RETURN s.uuid as uuid"
      )
      result = tx.run(query, id=identifier)
      if result.peek() == None:
        return False
      else:
        return True
#      try:

    # def find_person(self, person_name):
    #     with self.driver.session() as session:
    #         result = session.read_transaction(self._find_and_return_person, person_name)
    #         for row in result:
    #             print("Found person: {row}".format(row=row))

  @staticmethod
  def _study_designs(tx, uuid):
    results = []
    query = (
      "MATCH (s:Study {uuid: $uuid})-[:STUDY_DESIGN]->(sd:StudyDesign)"
      "RETURN sd"
    )
    result = tx.run(query, uuid=uuid)
    for row in result:
      results.append(StudyDesign.wrap(row['sd']))
    return results

  @staticmethod
  def _study_parameters(tx, uuid):
    query = (
      "MATCH (s:Study {uuid: $uuid}) "
      "WITH s " 
      "MATCH (s)-[:STUDY_TYPE]->(st:Code), "
      "(s)-[:STUDY_PHASE]->(sp:Code)"
      "RETURN st,sp"
    )
    result = tx.run(query, uuid=uuid)
    for row in result:
      return StudyParameters(**{ "studyType": Code.wrap(row['st']), "studyPhase": Code.wrap(row['sp']) })

  @staticmethod
  def _study_identifiers(tx, uuid):
    results = []
    query = (
      "MATCH (s:Study {uuid: $uuid})-[:STUDY_IDENTIFIER]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE]->(o:Organisation)-[:ORGANISATION_TYPE]->(c:Code)"
      "RETURN si,o,c"
    )
    result = tx.run(query, uuid=uuid)
    for row in result:
      identifier = StudyIdentifier.wrap(row['si'])
      identifier.studyIdentifierScope = Organisation.wrap(row['o'])
      identifier.studyIdentifierScope.organisationType = Code.wrap(row['c'])
      results.append(identifier)
    return results

  @staticmethod
  def _list(tx):
    results = []
    query = (
      "MATCH (s:Study)"
      "RETURN s.uuid as uuid"
    )
    result = tx.run(query)
    for row in result:
      results.append(row['uuid'])
    return results

  # @staticmethod
  # def _find_study(tx, the_uuid):
  #   query = (
  #     "MATCH (s:Study) "
  #     "WHERE s.uuid = $the_uuid "
  #     "RETURN s"
  #   )
  #   result = tx.run(query, the_uuid=the_uuid)
  #   return [row["s"] for row in result]
