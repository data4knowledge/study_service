from .node import Node
from .neo4j_connection import Neo4jConnection
from .file_nodes_and_edges import FileNodesAndEdges
from service.github_service import GithubService
from service.aura_service import AuraService

from uuid import uuid4
from usdm_excel import USDMExcel

import os
import yaml
import time

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
        self.error = f"Failed to create directory {e}"
        return False
      else:
        try:
          with open(self.full_path, 'wb') as f:
            f.write(contents)
        except Exception as e:
          self.error = f"Failed to save file content {e}"
          return False
        else:
          try:
            session.execute_write(self._create, self.filename, self.full_path, self.uuid)
          except Exception as e:
            self.error = f"Failed to save file details {e}"
            return False
          else:    
            return True

  def clear(self):
    db = Neo4jConnection()
    with db.session() as session:
      status = session.execute_write(self._set_status, self.uuid, 'initialised')

  def execute(self):
    try:
      excel = USDMExcel(self.full_path)
      nodes_and_edges = excel.to_neo4j_dict()
      filename = os.path.join("uploads", self.uuid, f"{self.uuid}.yaml")
      with open(f"{filename}", 'w') as f:
        f.write(yaml.dump(nodes_and_edges))

      print(f"EXE: Dump")
      ne = FileNodesAndEdges(self.dir_path, nodes_and_edges)
      ne.dump()

      print(f"EXE: Github")
      github = GithubService()
      file_list = github.upload_dir(self.uuid, self.dir_path, '*.csv')

      print(f"EXE: Wait")
      check = True
      loop_count = 0
      while check and (loop_count <= 12):
        file_count = github.file_count(self.uuid)
        print(f"EXE: File count {loop_count} {file_count} v {len(file_list)}")
        if file_count >= len(file_list):
          check = False
          print(f"EXE: File count done")
        else:
          time.sleep(5)
          loop_count += 1
          print(f"EXE: Sleep {loop_count}")

      print(f"EXE: Aura {file_list}")
      aura = AuraService()
      aura.load(file_list)

      return True
    except Exception as e:
      self.error = f"Failed to convert file {e}"
      return False

  # def execute(self):
  #   db = Neo4jConnection()
  #   with db.session() as session:
  #     status = session.execute_write(self._set_status, self.uuid, 'running')
  #     study = ImportExcel(self.full_path)
  #     identifier = study.identifier()
  #     if not identifier == None:
  #       session.execute_write(self._delete_study, study.identifier())
  #     result = study.save()
  #     #print("STUDY:", result)
  #     study = Study.find(result)
  #     study_designs = study.study_designs()
  #     study_design = study_designs[0]
  #     #print("STUDY DESIGN:", study_design)
  #     result = study_design.data_contract()
  #     result = study_design.sdtm_domains()
  #     status = session.execute_write(self._set_status, self.uuid, 'completed')
  #     return status

  @staticmethod
  def _create(tx, filename, full_path, uuid):
    query = """
      CREATE (df:StudyFile {filename: $filename, full_path: $full_path, uuid: $uuid, status: 'initialised'})
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

  @staticmethod
  def _find_by_path(tx, full_path):
    query = """
      MATCH (df:StudyFile {full_path: $full_path})
      RETURN df.uuid as uuid
    """
    results = tx.run(query, full_path=full_path)
    for row in results:
      return row['uuid']
    return None

  @staticmethod
  def _check_extension(filename):
    return True if filename.endswith('.xlsx') else False
  
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

