from .node import Node
from .neo4j_connection import Neo4jConnection
from .study_file_nodes_and_edges import StudyFileNodesAndEdges
from service.github_service import GithubService
from service.aura_service import AuraService
from uuid import uuid4
from usdm_excel import USDMExcel

import os
import yaml
import traceback
import logging

class StudyFile(Node):
  uuid: str = ""
  filename: str = ""
  full_path: str = ""
  dir_path: str = ""
  status: str = ""
  error: str = ""

  def create(self, filename, contents):
    if not self._check_extension(filename):
      self.error = "Invalid extension, must be '.xlsx'"
      return False
    self.filename = filename
    self.uuid = str(uuid4())
    self.dir_path = os.path.join("uploads", self.uuid)
    self.full_path = os.path.join("uploads", self.uuid, f"{self.uuid}.xlsx")
    db = Neo4jConnection()
    with db.session() as session:
      try:
        os.mkdir(self.dir_path)
      except Exception as e:
        self.error = f"Failed to create directory"
        self._log(e, f"{traceback.format_exc()}")
        return False
      else:
        try:
          with open(self.full_path, 'wb') as f:
            f.write(contents)
        except Exception as e:
          self.error = f"Failed to save file content"
          self._log(e, f"{traceback.format_exc()}")
          return False
        else:
          try:
            session.execute_write(self._create, self.filename, self.full_path, self.uuid)
          except Exception as e:
            self.error = f"Failed to save file details"
            self._log(e, f"{traceback.format_exc()}")
            return False
          else:
            self.set_status("Loaded")
            return True

  def set_status(self, status):
    self.status = status
    logging.info(f"Study load, status: {status}")
    #print(f"Study load, status: {status}")
    db = Neo4jConnection()
    with db.session() as session:
      session.execute_write(self._set_status, self.uuid, status)

  def execute(self):
    try:

      self.set_status("processing excel file")
      excel = USDMExcel(self.full_path)
      nodes_and_edges = excel.to_neo4j_dict()
      filename = os.path.join("uploads", self.uuid, f"{self.uuid}.yaml")
      with open(f"{filename}", 'w') as f:
        f.write(yaml.dump(nodes_and_edges))

      self.set_status("processing nodes")
      ne = StudyFileNodesAndEdges(self.dir_path, nodes_and_edges)
      ne.dump()

      self.set_status("uploading to github")
      github = GithubService()
      file_list = github.upload_dir(self.uuid, self.dir_path, '*.csv')

      self.set_status("loading database")
      aura = AuraService()
      aura.load(self.uuid, self.dir_path, file_list)

      self.set_status("completed")
      return True

    except Exception as e:
      self.error = f"Failed to process and load study. Was {self.status}"
      self._log(e, f"{traceback.format_exc()}")
      return False

  def _log(self, e, trace):
    logging.error(self.error)
    logging.error(f"Exception {e}\n{trace}")

  @staticmethod
  def _create(tx, filename, full_path, uuid):
    query = """
      CREATE (df:StudyFile {filename: $filename, full_path: $full_path, uuid: $uuid, status: 'Initialised'})
      RETURN df.uuid as uuid
    """
    results = tx.run(query, 
      filename=filename,
      full_path=full_path,
      uuid=uuid
    )
    for row in results:
      return row["uuid"]
    return None

  def _check_extension(self, filename):
    return True if filename.endswith('.xlsx') else False

  # @staticmethod
  # def _find_by_path(tx, full_path):
  #   query = """
  #     MATCH (df:StudyFile {full_path: $full_path})
  #     RETURN df.uuid as uuid
  #   """
  #   results = tx.run(query, full_path=full_path)
  #   for row in results:
  #     return row['uuid']
  #   return None

  @staticmethod
  def _set_status(tx, uuid, status):
    query = """
      MATCH (sf:StudyFile {uuid: $uuid})
      SET sf.status = $status
      RETURN sf
    """
    results = tx.run(query, uuid=uuid, status=status)
    for row in results:
      return StudyFile.wrap(row['sf'])
    return None
