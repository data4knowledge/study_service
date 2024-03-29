from .base_node import BaseNode
from d4kms_service import Neo4jConnection
from d4kms_generic import application_logger
from .study_file_nodes_and_edges import StudyFileNodesAndEdges
from service.github_service import GithubService
from service.aura_service import AuraService
from service.ra_service import RAService
from uuid import uuid4
from usdm_excel import USDMExcel
from model.study_design_data_contract import StudyDesignDataContract
from model.study_design_sdtm import StudyDesignSDTM

import os
import yaml
import traceback

class StudyFile(BaseNode):
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
      self.set_status("commencing", "Uploading file", 0)
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
            self.set_status("initialised", "Uploaded file", 0)
            return True

  def execute(self):
    try:

      self.set_status("running", "Processing excel file", 0)
      excel = USDMExcel(self.full_path)
      study = excel.the_study()
      nodes_and_edges = excel.to_neo4j_dict()
      filename = os.path.join("uploads", self.uuid, f"{self.uuid}.yaml")
      with open(f"{filename}", 'w') as f:
        f.write(yaml.dump(nodes_and_edges))

      self.set_status("running", "Converting data to database format", 10)
      ne = StudyFileNodesAndEdges(self.dir_path, nodes_and_edges)
      ne.dump()

      self.set_status("running", "Uploading to github", 15)
      git = GithubService()
      file_count = git.file_list(self.dir_path, "*.csv")
      for index in range(file_count):
        more = git.next()
        count = git.progress()
        percent = 15 + int(55.0 * (float(count) / float(file_count)))
        self.set_status("running", "Uploading to github", percent)
      git.load()

      self.set_status("running", "Loading database", 70)
      aura = AuraService()
      files = git.upload_file_list()
      application_logger.debug(f"Aura load: {self.uuid} {files[0]}")
      aura.load(self.uuid, files)

      self.set_status("running", "Creating data contract", 80)
      name = study.name
      ns = RAService().namespace_by_name('d4k Study namespace')
      #StudyDesignDataContract.create(name, ns['value'])

      self.set_status("running", "Adding SDTM domains", 90)
      study_design = study.versions[0].studyDesigns[0]
      print(f"SD: {study_design}")
      result = StudyDesignSDTM.create(study_design)

      self.set_status("complete", "Finished", 100)
      return True

    except Exception as e:
      self.error = f"Failed to process and load study. Was {self.status}"
      self._log(e, f"{traceback.format_exc()}")
      return False

  def set_status(self, status, stage, percentage):
    self.status = status
    application_logger.info(f"Study load, status: {status}")
    #print(f"Study load, status: {status}")
    db = Neo4jConnection()
    with db.session() as session:
      session.execute_write(self._set_status, self.uuid, status, stage, percentage)

  def get_status(self):
    db = Neo4jConnection()
    with db.session() as session:
      query = "MATCH (n:StudyFile {uuid: '%s'}) RETURN n.status as status, n.percentage as percent, n.stage as stage" % (self.uuid)
      result = session.run(query)
      for record in result:
        #print(f"RECORD: {record}")
        return {'status': record['status'], 'percentage': record['percent'], 'stage': record['stage'] }
    return ""
    
  def _log(self, e, trace):
    application_logger.error(self.error)
    application_logger.error(f"Exception {e}\n{trace}")

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
  def _set_status(tx, uuid, status, stage, percentage):
    query = """
      MATCH (sf:StudyFile {uuid: $uuid})
      SET sf.status = $status, sf.stage = $stage, sf.percentage = $percentage
      RETURN sf
    """
    results = tx.run(query, uuid=uuid, status=status, stage=stage, percentage=percentage)
    for row in results:
      return StudyFile.wrap(row['sf'])
    return None
