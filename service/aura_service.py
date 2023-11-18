from neo4j import GraphDatabase
from stringcase import pascalcase, snakecase
from utility.service_environment import ServiceEnvironment

import os
import glob
import traceback

class AuraService():

  def __init__(self):
    sv = ServiceEnvironment()
    self.database = sv.get('NEO4J_DB_NAME')
    self.url = sv.get('NEO4J_URI')
    self.usr = sv.get('NEO4J_USERNAME')
    self.pwd = sv.get('NEO4J_PASSWORD')
    self.driver = GraphDatabase.driver(self.url, auth=(self.usr, self.pwd))
    self.project_root = sv.get("GITHUB_BASE")

  def load(self, uuid, dir, file_list):
    print(f"AURA1: {file_list[:5]}")
    try:
      load_files = []
      for filename in file_list:
        #filename_parts = filename.split("/")
        #parts = filename_parts[2].split("-")
        print(f"AURA2: {filename}")
        parts = filename.split("-")
        file_path = os.path.join(self.project_root, dir, filename)
        print(f"AURA3: {file_path}")
        if parts[0] == "node":
          load_files.append({ "label": pascalcase(parts[1]), "filename": file_path })
        else:
          load_files.append({ "type": parts[1].upper(), "filename": file_path })
      result = None
      session = self.driver.session(database=self.database)
      nodes = []
      relationships = []
      print(f"AURA4: {load_files}")
      for file_item in load_files:
        if "label" in file_item:
          nodes.append("{ fileName: '%s', labels: ['%s'] }" % (file_item["filename"], file_item["label"]) )
        else:
          relationships.append("{ fileName: '%s', type: '%s' }" % (file_item["filename"], file_item["type"]) )
      query = """CALL apoc.import.csv( [%s], [%s], {stringIds: true})""" % (", ".join(nodes), ", ".join(relationships))
      print(f"AURA5: query={query}")        
      result = session.run(query)
      for record in result:
        print(record)
        return_value = {'nodes': record['nodes'], 'relationships': record['relationships'], 'time': record['time']}
      self.driver.close()
      return return_value
    except Exception as e:
      print(f"AURA: Upload exception {e}")
      print(f"{traceback.format_exc()}")
