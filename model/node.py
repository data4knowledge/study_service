from pydantic import BaseModel
from model.neo4j_connection import Neo4jConnection

class Node(BaseModel):

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

