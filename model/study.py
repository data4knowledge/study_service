from pydantic import BaseModel
from typing import List, Union
from model.code import *
from model.study_identifier import *
from model.scoped_identifier import *
from model.study_protocol_version import *
from model.study_design import *
from model.neo4j_connection import Neo4jConnection
from model.semantic_version import SemanticVersion
from model.node import Node

from uuid import UUID, uuid4

class StudyIn(BaseModel):
  title: str
  identifier: str

class StudyOut(BaseModel):
  uuid = str
  uri = str
  studyTitle: str
  studyVersion: str
  studyType: dict
  studyPhase: dict
  studyIdentifiers: dict
  studyProtocolVersions: dict
  studyDesigns: dict

class StudyList(BaseModel):
  items: List[str]
  page: int
  size: int
  filter: str
  count: int

class StudyDesignList(BaseModel):
  items: List[str]
  page: int
  size: int
  filter: str
  count: int

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

  # @classmethod
  # def find(cls, uuid):
  #   db = Neo4jConnection()
  #   with db.session() as session:
  #     results = session.execute_read(cls._find_study, uuid)
  #     if len(results) == 1:
  #       return results[0]
  #     elif len(results) == 0:
  #       return None
  #     else:
  #       return None
        
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
  def list(cls):
    db = Neo4jConnection()
    with db.session() as session:
      results = {'items': [], 'page': 1, 'size': 0, 'filter': "", 'count': 0 }
      results['items'] = session.execute_read(cls._list)
      results['count'] = len(results['items'])
      results['size'] = len(results['items'])
      return results


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

  @staticmethod
  def _create_study(tx, identifier, title):
      semantic_version = SemanticVersion().draft()
      query = (
        "CREATE (s:Study { studyTitle: $title, uuid: $uuid1 })"
        "CREATE (si:ScopedIdentifier { identifier: $id, version: 1, semantic_version: $sv, uuid: $uuid2 })"
        "CREATE (sd:StudyDesign { uuid: $uuid3 })"
        "CREATE (sc:StudyCell { uuid: $uuid4 })"
        "CREATE (sa:StudyArm { uuid: $uuid5 })"
        "CREATE (se:StudyEpoch { uuid: $uuid6 })"
        "CREATE (wf:Workflow { uuid: $uuid7, name: 'SoA', description: 'The SoA workflow' })"
        "CREATE (s)-[:IDENTIFIED_BY]->(si)"
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
        sv=semantic_version,
        uuid1=str(uuid4()), 
        uuid2=str(uuid4()),
        uuid3=str(uuid4()),
        uuid4=str(uuid4()),
        uuid5=str(uuid4()),
        uuid6=str(uuid4()),
        uuid7=str(uuid4()),
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
      query = (
        "MATCH (s:Study { uuid: $uuid1 })-[:STUDY_DESIGN|IDENTIFIED_BY|STUDY_CELL|STUDY_ARM|STUDY_EPOCH|STUDY_WORKFLOW  *1..]->(n)"
        "DETACH DELETE (n)"
        "DETACH DELETE (s)"
      )
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
      "RETURN sd.uuid as uuid"
    )
    result = tx.run(query, uuid=uuid)
    for row in result:
      results.append(row['uuid'])
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
