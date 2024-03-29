from neo4j import GraphDatabase
from stringcase import pascalcase, snakecase
from d4kms_generic import ServiceEnvironment
from d4kms_generic import application_logger

import os
import traceback
import logging

class AuraService():

  class UploadFail(Exception):
    pass

  def __init__(self):
    sv = ServiceEnvironment()
    self.database = sv.get('NEO4J_DB_NAME')
    self.url = sv.get('NEO4J_URI')
    self.usr = sv.get('NEO4J_USERNAME')
    self.pwd = sv.get('NEO4J_PASSWORD')
    self.driver = GraphDatabase.driver(self.url, auth=(self.usr, self.pwd))
    self.project_root = sv.get("GITHUB_BASE")

  def load(self, dir, file_list):
    try:
      load_files = []
      for filename in file_list:
        application_logger.debug(f"load file: {self.project_root}, {filename}")
        parts = filename.split("-")
        file_path = os.path.join(self.project_root, dir, filename)
        if parts[0] == "node":
          load_files.append({ "label": pascalcase(parts[1]), "filename": file_path })
        else:
          load_files.append({ "type": parts[1].upper(), "filename": file_path })
      result = None
      session = self.driver.session(database=self.database)
      nodes = []
      relationships = []
      for file_item in load_files:
        if "label" in file_item:
          nodes.append("{ fileName: '%s', labels: ['%s'] }" % (file_item["filename"], file_item["label"]) )
        else:
          relationships.append("{ fileName: '%s', type: '%s' }" % (file_item["filename"], file_item["type"]) )
      query = """CALL apoc.import.csv( [%s], [%s], {stringIds: true})""" % (", ".join(nodes), ", ".join(relationships))
      application_logger.debug(f"QUERY: {query}")
      result = session.run(query)
      for record in result:
        return_value = {'nodes': record['nodes'], 'relationships': record['relationships'], 'time': record['time']}
      self.driver.close()
      application_logger.info(f"Loaded Aura, details: {return_value}")
      return return_value
    except Exception as e:
      application_logger.error(f"Exception raised while uploading to Aura database")
      application_logger.error(f"Exception {e}\n{traceback.format_exc()}")
      raise self.UploadFail

  def load_identifiers(self, dir, filename):
    try:
      # load_files = []
      # for filename in file_list:
      #   application_logger.debug(f"load file: {self.project_root}, {filename}")
      #   parts = filename.split("-")
      #   file_path = os.path.join(self.project_root, dir, filename)
      #   if parts[0] == "node":
      #     load_files.append({ "label": pascalcase(parts[1]), "filename": file_path })
      #   else:
      #     load_files.append({ "type": parts[1].upper(), "filename": file_path })
      # result = None
      session = self.driver.session(database=self.database)
      print("Going to import subject and site with filename:",filename)
      file_path = os.path.join(self.project_root, dir, filename)
      print("file_path:",file_path)
      query = f"""
          LOAD CSV WITH HEADERS FROM '{file_path}' AS site_row
          MATCH (design:StudyDesign {{name:'Study Design 1'}})
          MERGE (s:Subject {{identifier:site_row['USUBJID']}})
          MERGE (site:StudySite {{name:site_row['SITEID']}})
          MERGE (s)-[:ENROLLED_AT_SITE_REL]->(site)
          MERGE (site)<-[:MANAGES_SITE]-(researchOrg)
          MERGE (researchOrg)<-[:ORGANIZATIONS_REL]-(design)
          RETURN count(*)
      """
      application_logger.debug(f"QUERY: {query}")
      result = session.run(query)
      data = [x.data() for x in result]
      print("---data",data)
      # for record in result:
      #   return_value = {'nodes': record['nodes'], 'relationships': record['relationships'], 'time': record['time']}
      self.driver.close()
      application_logger.info(f"Loaded Aura, details: {return_value}")
      return True
    except Exception as e:
      application_logger.error(f"Exception raised while uploading to Aura database")
      application_logger.error(f"Exception {e}\n{traceback.format_exc()}")
      raise self.UploadFail