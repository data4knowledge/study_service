from pydantic import BaseModel
from typing import List, Union
from .node import NodeNameLabelDesc
from .neo4j_connection import Neo4jConnection

class Study(NodeNameLabelDesc):

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
          MATCH (n:Study) RETURN n ORDER BY n.name DESC %s
        """ % (skip_offset_clause)
      else:
        parent_filter_clause = cls.build_filter_clause(filter, cls.parent_properties())
        query = """
          MATCH(n:Study) %s RETURN n
          }
        """ % (parent_filter_clause, skip_offset_clause)
      query_results = session.run(query)
      for query_result in query_results:
        rel = dict(query_result['n'])
        results.append(rel)
      return results

  @classmethod
  def full_count(cls):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (n:Study) RETURN COUNT(n) as count 
      """
      query_results = session.run(query)
      for result in query_results:
        return result['count']
      return 0

  @classmethod
  def filter_count(cls, filter):
    db = Neo4jConnection()
    with db.session() as session:
      parent_filter_clause = cls.build_filter_clause(filter, cls.parent_properties())
      query = """
          MATCH(n:Study) %s RETURN n
      """ % (parent_filter_clause)
      #print(query)
      query_results = session.run(query)
      return len(query_results.data())

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

class StudyList(BaseModel):
  items: List[Study]
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

