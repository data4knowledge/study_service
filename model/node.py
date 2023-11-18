from pydantic import BaseModel
from model.neo4j_connection import Neo4jConnection

class Node(BaseModel):
  uuid: str

  @classmethod
  def wrap(cls, node):
    dict = {}
    for items in node.items():
      dict[items[0]] = items[1]
    return cls(**dict)

  @classmethod
  def find(cls, uuid):
    db = Neo4jConnection()
    with db.session() as session:
      return session.execute_read(cls._find, cls, uuid)

  @staticmethod
  def _find(tx, cls, uuid):
    query = """
      MATCH (a:%s { uuid: $uuid1 })
      RETURN a
    """ % (cls.__name__)
    result = tx.run(query, uuid1=uuid)
    for row in result:
      dict = {}
      for items in row['a'].items():
        dict[items[0]] = items[1]
      return cls(**dict)
    return None

  @classmethod
  def build_filter_clause(cls, filter, properties):
    filter_clause_parts = []
    for property in properties:
      filter_clause_parts.append("toLower(%s) CONTAINS toLower('%s')" % (property, filter))
    filter_clause = " OR ".join(filter_clause_parts)
    filter_clause = "WHERE %s " % (filter_clause)
    return filter_clause

  @classmethod
  def list(cls, page, size, filter):
    if filter == "":
      count = cls.full_count()
    else:
      count = cls.filter_count(filter)
    results = {'items': [], 'page': page, 'size': size, 'filter': filter, 'count': count }
    results['items'] = cls.list_items(page, size, filter)
    return results

  @classmethod
  def list_items(cls, page=0, size=0, filter=""):
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
          MATCH (n:%s) RETURN n ORDER BY n.name DESC %s
        """ % (cls.__name__, skip_offset_clause)
      else:
        parent_filter_clause = cls.build_filter_clause(filter, cls.parent_properties())
        query = """
          MATCH(n:%s) %s RETURN n
          }
        """ % (cls.__name__, parent_filter_clause, skip_offset_clause)
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
        MATCH (n:%s) RETURN COUNT(n) as count 
      """ % (cls.__name__)
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
          MATCH(n:%s) %s RETURN n
      """ % (cls.__name__, parent_filter_clause)
      #print(query)
      query_results = session.run(query)
      return len(query_results.data())

class NodeId(Node):
  id: str

class NodeName(NodeId):
  name: str

  @classmethod
  def parent_properties(cls):
    return ["n.name"]
  
class NodeNameLabel(NodeId):
  name: str
  label: str

  @classmethod
  def parent_properties(cls):
    return ["n.name", "n.label"]

class NodeNameLabelDesc(NodeId):
  name: str
  description: str
  label: str

  @classmethod
  def parent_properties(cls):
    return ["n.name", "n.description", "n.label"]
