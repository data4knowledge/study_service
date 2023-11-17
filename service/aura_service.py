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

  def load(self, file_list):
    print(f"AURA: {file_list}")
    try:
      load_files = []
      for filename in file_list:
        parts = filename.replace("load_data/", "").split("-")
        file_path = os.path.join(self.project_root, filename)
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
      query = """CALL apoc.import.csv( [%s], [%s], {stringIds: false})""" % (", ".join(nodes), ", ".join(relationships))
      result = session.run(query)
      for record in result:
        print(record)
        return_value = {'nodes': record['nodes'], 'relationships': record['relationships'], 'time': record['time']}
      self.driver.close()
      return return_value
    except Exception as e:
      print(f"AURA: Upload exception {e}")
      print(f"{traceback.format_exc()}")
