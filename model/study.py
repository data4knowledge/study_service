from pydantic import BaseModel
from typing import List, Union
from model.code import *
from model.study_identifier import *
from model.scoped_identifier import *
from model.study_protocol_version import *
from model.study_design import *
from model.neo4j_connection import Neo4jConnection
from model.semantic_version import SemanticVersion
from uuid import UUID, uuid4

class StudyIn(BaseModel):
  title: str
  identifier: str

class Study(BaseModel):
  uuid = str
  uri = str
  studyTitle: str
  studyVersion: str
  studyType: Union[Code, UUID, None]
  studyPhase: Union[Code, UUID, None]
  studyIdentifiers: Union[List[StudyIdentifier], List[UUID], None]
  studyProtocolVersions: Union[List[StudyProtocolVersion], List[UUID], None]
  studyDesigns: Union[List[StudyDesign], List[UUID], None]

  #identified_by = Union[ScopedIdentifier, UUID, None]
  
# class StudyPartial(BaseModel):
#   uri: str
#   uuid: str
#   name: str
#   identified_by: dict
#   has_status: dict

# class StudyList(BaseModel):
#   items: List[StudyPartial]
#   page: int
#   size: int
#   filter: str
#   count: int

#   @classmethod
#   def list(cls, page, size, filter):
#     if filter == "":
#       count = bci.full_count()
#     else:
#       count = bci.filter_count(filter)
#     results = {'items': [], 'page': page, 'size': size, 'filter': filter, 'count': count }
#     results['items'] = bci.list(page, size, filter)
#     return results

# class Study(BaseModel):
#   uri: str
#   uuid: str
#   name: str
#   identified_by: dict
#   has_status: dict
#   #identifier: dict
#   items: List[dict]

#   @classmethod
#   def find(cls, uuid):
#     return bci.find(uuid)

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

  @staticmethod
  def _create_study(tx, identifier, title):
      semantic_version = SemanticVersion().draft()
      query = (
        "CREATE (s:Study { studyTitle: $title, uuid: $uuid1 })"
        "CREATE (si:ScopedIdentifier { identifier: $id, version: 1, semantic_version: $sv, uuid: $uuid2 })"
        "CREATE (sd:StudyDesign)"
        "CREATE (sc:StudyCell)"
        "CREATE (sa:StudyArm)"
        "CREATE (se:StudyEpoch)"
        "CREATE (wf:Workflow)"
        "CREATE (s)-[:IDENTIFIED_BY]->(si)"
        "CREATE (s)-[:STUDY_DESIGN]->(sd)"
        "CREATE (sd)-[:STUDY_CELL]->(sc)"
        "CREATE (sd)-[:STUDY_WORKFLOW]->(wf)"
        "CREATE (sc)-[:STUDY_ARM]->(sa)"
        "CREATE (sc)-[:STUDY_EPOCH]->(se)"
        "RETURN s.uuid as uuid"
      )
      result = tx.run(query, title=title, id=identifier, uuid1=str(uuid4()), uuid2=str(uuid4()), sv=semantic_version)
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

    # @staticmethod
    # def _find_and_return_person(tx, person_name):
    #     query = (
    #         "MATCH (p:Person) "
    #         "WHERE p.name = $person_name "
    #         "RETURN p.name AS name"
    #     )
    #     result = tx.run(query, person_name=person_name)
    #     return [row["name"] for row in result]
