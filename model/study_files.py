from typing import List
from pydantic import BaseModel
from .neo4j_connection import Neo4jConnection
from .study_file import StudyFile

class StudyFiles(BaseModel):
  items: List[StudyFile]
  page: int
  size: int
  filter: str
  count: int
  
  @classmethod
  def list(cls, page, size, filter):
    count = cls.full_count()
    results = {'items': [], 'page': page, 'size': size, 'filter': filter, 'count': count }
    results['items'] = cls.get_list(page, size, filter)
    return results

  @classmethod
  def full_count(cls):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (n:StudyFile) RETURN COUNT(n) as count
      """ 
      query_results = session.run(query)
      for query_result in query_results:
        return query_result['count']
      return 0

  @classmethod
  def get_list(cls, page=0, size=0, filter=""):
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      skip_offset_clause = ""
      if page != 0:
        offset = (page - 1) * size
        skip_offset_clause = "SKIP %s LIMIT %s" % (offset, size)
      query = """
        MATCH (n:StudyFile) %s RETURN n
      """ % (skip_offset_clause)
      query_results = session.run(query)
      for query_result in query_results:
        results.append(dict(query_result['n']))
      return results

