import os
from d4kms_service import Node, Neo4jConnection
from model.base_node import BaseNode
from typing import List, Literal
from uuid import uuid4
import pandas as pd
from d4kms_generic import application_logger

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

  @classmethod
  def add_parent_activities(cls, full_path, study_design_uuid):
    # @NOTE: Workaround: Get create parent activities as they are not in usdm v3
    try:
      xl = pd.read_excel(full_path, sheet_name='mainTimeline')
      parent_activities = {}
      FIRST_ACTIVITY_ROW = 8
      parent = None
      child = None
      try:
        for x in xl.to_dict(orient="records")[FIRST_ACTIVITY_ROW:]:
          if x['Name'] and isinstance(x['Name'], str) and len(x['Name']) > 2:
            parent = x['Name'].strip()
            child = x['Main Timeline'].strip()
            if parent in parent_activities:
              parent_activities[parent].append(child)
            else:
              parent_activities[parent] = [child]
          # else:
          #   print("Ignoring x['Name']", x['Name'], x['Name'].__class__)
        db = Neo4jConnection()
        with db.session() as session:
          result = session.execute_write(cls._add_parent_activities, parent_activities, study_design_uuid)
        db.close()
      except Exception as e:
        application_logger.info(f"Could not parse parent activities parent: {parent} child: {child} {e}")
        return False
    except Exception as e:
      application_logger.info(f"Could not read mainTimeline {e}")
      return False
    return True

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


  @staticmethod
  def _add_parent_activities(tx, parent_activities, study_design_uuid):
    for parent, children in parent_activities.items():
      parent_uuid = str(uuid4())
      query = """
          CREATE (p:Activity {name: '%s', label: "Parent activity", description: "Parent activity", uuid: '%s', instanceType: 'Activity'})
          RETURN p
      """ % (parent, parent_uuid)
      # print("query", query)
      result = tx.run(query)
      parent_created = False
      for record in result:
        parent_created = True

      if parent_created:
        application_logger.info(f"Parent activity created {parent}")
        for child in children:
          query = """
              MATCH (sd:StudyDesign {uuid:'%s'})-[:ACTIVITIES_REL]->(c:Activity {name: '%s'})
              MATCH (p:Activity {uuid: '%s'})
              MERGE (p)-[:CHILD_REL]->(c)
              RETURN sd, p, c
          """ % (study_design_uuid, child, parent_uuid)
          # print(f"child: '{child}'")
          result = tx.run(query)
          child_created = False
          for record in result:
            child_created = True
          if not child_created:
            application_logger.warning(f"Failed to create child activity {child}")
            print("query", query)
    return ""



class Configuration():

  def __init__(self, *a, **kw):
    super().__init__(*a, **kw)
