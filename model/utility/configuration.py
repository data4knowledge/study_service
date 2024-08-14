import os
from d4kms_service import Node, Neo4jConnection
from model.base_node import BaseNode
from typing import List, Literal

class ConfigurationNode(BaseNode):
  name: str = ""
  study_products_bcs: List[str]= []
  disposition: List[str]= []
  demography: List[str]= []

  @classmethod
  def get(cls):
    db = Neo4jConnection()
    with db.session() as session:
      result = session.execute_read(cls._get, cls)
    db.close()
    return result

  @staticmethod
  def _get(tx, cls):
    query = """
      MATCH (a:%s) RETURN a
    """ % (cls.__name__)
    result = tx.run(query)
    for row in result:
      return cls.wrap(row['a'])
    print("Could not get configuration")
    return cls.wrap({"uuid":"failed"})

class Configuration():

  def __init__(self, *a, **kw):
    super().__init__(*a, **kw)
