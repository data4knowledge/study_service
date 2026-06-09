from neo4j import GraphDatabase
from stringcase import pascalcase
from d4kms_generic import ServiceEnvironment
from d4kms_generic import application_logger
from model.utility.raw_data import read_raw_data_file

import os
import csv

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
      # load_files = []
      application_logger.info(f"Loading files")
      nodes = []
      relationships = []
      for filename_entry in file_list:
        filename = filename_entry['filename']
        file_path = filename_entry['file_path']
        # application_logger.debug(f"load file: {filename} from {file_path}")
        parts = filename.split("-")
        #file_path = os.path.join(self.project_root, dir, filename)
        if parts[0] == "node":
          nodes.append({'filename': filename, 'file_path': file_path, 'label': pascalcase(parts[1])})
        else:
          relationships.append({'filename': filename, 'file_path': file_path, 'type': parts[1].upper()})
      result = None

      # Need to read node files to get properties
      path = os.getcwd() + "/uploads/" + dir + "/"

      application_logger.info("loading nodes")
      for file_item in nodes:
        header = self.get_header_from_csv(path+file_item['filename'])
        status = self.load_nodes(file_item['label'], file_item['file_path'], header)

      application_logger.info("loading relationships")
      for file_item in relationships:
        status = self.load_relationships(file_item["type"], file_item["file_path"])

      application_logger.info(f"Loaded Aura, details: nodes and relationships")
      return_value = "nodes and relationships"
      return return_value
    except Exception as e:
      application_logger.exception(f"Exception raised while uploading to Aura database", e)
      raise self.UploadFail

  def get_header_from_csv(self, filename):
      with open(filename) as f:
          reader = csv.reader(f)
          header = next(reader) 
      return header


  def load_nodes(self, label, file_path, header):
    properties = "\n".join([f"set n.{p} = row['{p}']" for p in header if p != "uuid:ID"])
    session = self.driver.session(database=self.database)
    query = """
        LOAD CSV WITH HEADERS FROM '%s' AS row
        with row
        MERGE (n:%s {uuid:row['uuid:ID']})
        %s
        RETURN count(*) as count
    """ % (file_path, label, properties)
    # application_logger.debug(f"QUERY: {query}")
    results = session.run(query)
    count = [result.data() for result in results]
    self.driver.close()
    application_logger.info(f"Loaded Aura, details {label}: {count}")
    return True

  def load_relationships(self, type, file_path):
    session = self.driver.session(database=self.database)
    query = """
        LOAD CSV WITH HEADERS FROM '%s' AS row
        with row
        MATCH (n1 {uuid: row[':START_ID']})
        MATCH (n2 {uuid: row[':END_ID']})
        MERGE (n1)-[:%s]->(n2)
        RETURN count(*) as count
    """ % (file_path, type)
    # application_logger.debug(f"QUERY: {query}")
    results = session.run(query)
    count = [result.data() for result in results]
    self.driver.close()
    application_logger.info(f"Loaded Aura, details {file_path}: {count}")
    return True

  def load_identifiers(self, file_path, sd_uuid):
    try:
      raw_data = read_raw_data_file(file_path)
      load_subjects = [x['SUBJID'] for x in raw_data]
      print("load_subjects", load_subjects)
      load_site_ids = list(set([str(x['SITEID']) for x in raw_data]))
      with self.driver.session(database=self.database) as session:
        # Check if site exists, if not, create it
        query = f"""
            MATCH (design:StudyDesign {{uuid: '{sd_uuid}'}})-[:ORGANIZATIONS_REL]->(resOrg:ResearchOrganization)-[:MANAGES_REL]->(site:StudySite)
            WHERE site.name in {load_site_ids}
            RETURN distinct site.name as site_name
        """
        # print("query", query)
        result = session.run(query)
        existing_sites = [x['site_name'] for x in result.data()]
        new_sites = [x for x in load_site_ids if x not in existing_sites]
        print("new_sites", new_sites)
        for site_id in new_sites:
          query = f"""
              MATCH (design:StudyDesign {{uuid: '{sd_uuid}'}})-[:ORGANIZATIONS_REL]->(resOrg:ResearchOrganization)
              CREATE (site:StudySite {{name: '{site_id}'}})
              CREATE (resOrg)-[:MANAGES_REL]->(site)
              RETURN site.name as name
          """
          # print("query", query)
          result = session.run(query)
          application_logger.info(f"Added site: {result.data()}")

        # Check if subject exists, if not, create it
        query = f"""
            LOAD CSV WITH HEADERS FROM '{file_path}' AS site_row
            MATCH (design:StudyDesign {{uuid: '{sd_uuid}'}})-[:ORGANIZATIONS_REL]->(resOrg:ResearchOrganization)-[:MANAGES_REL]->(site:StudySite {{name:site_row['SITEID']}})
            MERGE (s:Subject {{identifier:site_row['SUBJID']}})-[:ENROLLED_AT_SITE_REL]->(site)
            RETURN count(*) as count
        """
        application_logger.debug(f"QUERY: {query}")
        result = session.run(query)
        for record in result:
          return_value = {'subjects': record['count']}
      application_logger.info(f"Loaded Aura, details: {return_value}")
      return True
    except Exception as e:
      application_logger.exception(f"Exception raised while uploading to Aura database", e)
      raise self.UploadFail

  def load_datapoints(self, file_path, sd_uuid):
    try:
      session = self.driver.session(database=self.database)
      query = f"""
          LOAD CSV WITH HEADERS FROM '{file_path}' AS data_row
          MATCH (dc:DataContract {{uri:data_row['DC_URI']}})
          MATCH (design:StudyDesign {{uuid: '{sd_uuid}'}})-[:ORGANIZATIONS_REL]->(resOrg:ResearchOrganization)-[:MANAGES_REL]->(site:StudySite)<-[:ENROLLED_AT_SITE_REL]-(s:Subject {{identifier: data_row['SUBJID']}})
          MERGE (d:DataPoint {{uri: data_row['DATAPOINT_URI'], value: data_row['VALUE']}})
          MERGE (record:Record {{key:data_row['RECORD_KEY']}})
          MERGE (dc)<-[:FOR_DC_REL]-(d)
          MERGE (d)-[:FOR_SUBJECT_REL]->(s)
          MERGE (d)-[:SOURCE]->(record)
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
