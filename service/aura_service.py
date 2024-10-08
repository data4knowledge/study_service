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
    #self.project_root = sv.get("GITHUB_BASE")

  def load(self, dir, file_list):
    try:
      load_files = []
      for filename_entry in file_list:
        filename = filename_entry['filename']
        file_path = filename_entry['file_path']
        # application_logger.debug(f"load file: {filename} from {file_path}")
        parts = filename.split("-")
        #file_path = os.path.join(self.project_root, dir, filename)
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
      #application_logger.debug(f"QUERY: {query}")
      result = session.run(query)
      for record in result:
        return_value = {'nodes': record['nodes'], 'relationships': record['relationships'], 'time': record['time']}
      self.driver.close()
      application_logger.info(f"Loaded Aura, details: {return_value}")
      return return_value
    except Exception as e:
      application_logger.exception(f"Exception raised while uploading to Aura database", e)
      raise self.UploadFail

  def load_identifiers(self, file_path):
    try:
      session = self.driver.session(database=self.database)
      query = f"""
          LOAD CSV WITH HEADERS FROM '{file_path}' AS site_row
          MATCH (design:StudyDesign {{name:'Study Design 1'}})
          MERGE (s:Subject {{identifier:site_row['USUBJID']}})
          MERGE (site:StudySite {{name:site_row['SITEID']}})
          MERGE (s)-[:ENROLLED_AT_SITE_REL]->(site)
          MERGE (site)<-[:MANAGES_SITE]-(researchOrg)
          MERGE (researchOrg)<-[:ORGANIZATIONS_REL]-(design)
          RETURN count(*) as count
      """
      # application_logger.debug(f"QUERY: {query}")
      result = session.run(query)
      for record in result:
        return_value = {'subjects': record['count']}
      self.driver.close()
      application_logger.info(f"Loaded Aura, details: {return_value}")
      return True
    except Exception as e:
      application_logger.exception(f"Exception raised while uploading to Aura database", e)
      raise self.UploadFail

  def load_datapoints(self, file_path):
    try:
      session = self.driver.session(database=self.database)
      query = f"""
          LOAD CSV WITH HEADERS FROM '{file_path}' AS data_row
          MATCH (dc:DataContract {{uri:data_row['DC_URI']}})
          MATCH (design:StudyDesign {{name:'Study Design 1'}})
          MERGE (d:DataPoint {{uri: data_row['DATAPOINT_URI'], value: data_row['VALUE']}})
          MERGE (s:Subject {{identifier:data_row['USUBJID']}})
          MERGE (dc)<-[:FOR_DC_REL]-(d)
          MERGE (d)-[:FOR_SUBJECT_REL]->(s)
          RETURN count(*) as count
      """
      # application_logger.debug(f"QUERY: {query}")
      result = session.run(query)
      for record in result:
        return_value = {'datapoints': record['count']}
      self.driver.close()
      application_logger.info(f"Loaded Aura, details: {return_value}")
      return True
    except Exception as e:
      application_logger.exception(f"Exception raised while uploading to Aura database", e)
      raise self.UploadFail
