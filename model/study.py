from typing import List
from pydantic import BaseModel
from typing import List, Union
from model.code import *
from model.study_identifier import *
from model.study_protocol_version import *
from model.study_design import *
from model.neo4j_connection import Neo4jConnection
from uuid import uuid4

class StudyIn(ApiBaseModel):
  title: str
  identifier: str

class Study(BaseModel):
  uuid: Union[UUID, None]
  studyTitle: str
  studyVersion: str
  studyType: Union[Code, UUID, None]
  studyPhase: Union[Code, UUID, None]
  studyIdentifiers: Union[List[StudyIdentifier], List[UUID], None] = []
  studyProtocolVersions: Union[List[StudyProtocolVersion], List[UUID], None] = []
  studyDesigns: Union[List[StudyDesign], List[UUID], None] = []

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
      result = session.write_transaction(cls._create_study, identifier, title)
      #return result
      return uuid4()

  @staticmethod
  def _create_study(tx, identifier, title):
      print("CREATE")
      query = (
        "CREATE (s:Study { studyTitle: $title, studyVersion: '0.1' }) RETURN s"
      )
      result = tx.run(query, title=title)
#      try:
      for row in result:
        print(row)
        return row["s"]
      return None
      # Capture any errors along with the query and data for traceability
      # except ServiceUnavailable as exception:
      #   logging.error("{query} raised an error: \n {exception}".format(
      #     query=query, exception=exception))
      #   raise

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
