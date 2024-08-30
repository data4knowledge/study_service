import os
from d4kms_service import Node, Neo4jConnection
from model.base_node import BaseNode
from typing import List, Literal
from uuid import uuid4

DEFAULT_CONFIGURATION = {
  'study_product_bcs': ["Exposure Unblinded"],
  'disposition'      : ["first_exposure", "last_exposure"],
  'demography'       : ['exposure_dates'],
  'crm_start'        : "--STDTC",
  'crm_end'          : "--ENDTC",
  'mandatory_bcs'    : ["Informed Consent Obtained"],
  'default_organization_id'    : "Organization_1",
}



class ConfigurationNode(BaseNode):
  name: str = ""
  study_product_bcs: List[str]= []
  disposition: List[str]= []
  demography: List[str]= []
  crm_start: str = ""
  crm_end: str = ""
  mandatory_bcs: List[str] = []
  default_organization_id: str = "Organization_1"

  def create(cls, params):
    db = Neo4jConnection()
    with db.session() as session:
      result = session.execute_write(cls._delete_configuration, cls)
      result = session.execute_write(cls._create, cls, params)
    db.close()
    return result

  @classmethod
  def create_default_configuration(cls):
    db = Neo4jConnection()
    with db.session() as session:
      result = session.execute_write(cls._delete_configuration, cls)
      result = session.execute_write(cls._create, cls, DEFAULT_CONFIGURATION)
    db.close()
    return result

  @classmethod
  def get(cls):
    db = Neo4jConnection()
    with db.session() as session:
      result = session.execute_read(cls._get, cls)
    db.close()
    return result

  @staticmethod
  def _create(tx, cls, source_params):
    params = []
    for param in source_params:
      if isinstance(source_params[param], list):
        params.append(f"a.{param}={source_params[param]}")
      else:
        params.append(f"a.{param}='{source_params[param]}'")
    params_str = ", ".join(params)
    query = """
      CREATE (a:%s {uuid: $uuid1})
      SET %s 
      RETURN a
    """ % (cls.__name__, params_str)
    # print("query",query)
    result = tx.run(query, uuid1=str(uuid4()))
    for row in result:
      print("Configuration node created",row['a'])
      return {'items': cls.wrap(row['a'])}
    print("Failed to create configuration node")
    return None

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

  @staticmethod
  def _delete_configuration(tx, cls):
    query = """
      MATCH (a:%s) detach delete a
      RETURN count(a) as count
    """ % (cls.__name__)
    result = tx.run(query, uuid1=str(uuid4()))
    for row in result:
      print("Deleted old configuration node(s)",row['count'])
    return None


class Configuration():

  def __init__(self, *a, **kw):
    super().__init__(*a, **kw)
