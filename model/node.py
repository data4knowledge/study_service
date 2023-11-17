from pydantic import BaseModel
from model.neo4j_connection import Neo4jConnection

class Node(BaseModel):

  @classmethod
  def wrap(cls, node):
    dict = {}
    for items in node.items():
      dict[items[0]] = items[1]
    return cls(**dict)

  @classmethod
  def build_filter_clause(cls, filter, properties):
    filter_clause_parts = []
    for property in properties:
      filter_clause_parts.append("toLower(%s) CONTAINS toLower('%s')" % (property, filter))
    filter_clause = " OR ".join(filter_clause_parts)
    filter_clause = "WHERE %s " % (filter_clause)
    return filter_clause

class NodeId(Node):
  id: str
  uuid: str

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

class NodeNameLabelDesc(NodeId):
  name: str
  description: str
  label: str

  @classmethod
  def parent_properties(cls):
    return [
      "n.name", "n.description", "n.label"
    ]
