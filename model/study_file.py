from .base_node import BaseNode
from d4kms_service import Neo4jConnection
from d4kms_generic import application_logger
from d4kms_generic import ServiceEnvironment
from .study_file_nodes_and_edges import StudyFileNodesAndEdges
from service.github_service import GithubService
from service.dropbox_service import DropboxService
from service.local_service import LocalService
from service.aura_service import AuraService
from service.ra_service import RAService
from uuid import uuid4
from usdm_db import USDMDb
from model.study_design_data_contract import StudyDesignDataContract
from model.study_design_sdtm import StudyDesignSDTM
from model.study_design_bc import StudyDesignBC

import os
import yaml
import traceback
import datetime

class StudyFile(BaseNode):
  uuid: str = ""
  filename: str = ""
  full_path: str = ""
  dir_path: str = ""
  status: str = ""
  stage: str = ""
  percentage: int = 0
  error: str = ""
  service: str = ""
  date_time: str = ""
  upload_service: str = None

  def __init__(self, *a, **kw):
    super().__init__(*a, **kw)
    se = ServiceEnvironment()
    self.upload_service = se.get('UPLOAD_SERVICE')

  @classmethod
  def list(cls, page, size, filter):
    return cls.base_list("MATCH (n:StudyFile)", "ORDER BY n.name ASC", page, size, filter)
  
  def create(self, filename, contents):
    if not self._check_extension(filename):
      self.error = "Invalid extension, must be '.xlsx'"
      return False
    self.filename = filename
    self.uuid = str(uuid4())
    self.dir_path = os.path.join("uploads", self.uuid)
    self.full_path = os.path.join("uploads", self.uuid, f"{self.uuid}.xlsx")
    self.status = "commencing"
    self.stage = "Uploading file"
    self.percentage = 0
    self.service = "Github"
    self.date_time = datetime.datetime.now().replace(microsecond=0).isoformat()
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
            session.execute_write(self._create, self)
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
      usdm = USDMDb()
      errors = usdm.from_excel(self.full_path)
      study = usdm.wrapper().study
      study_design = study.versions[0].studyDesigns[0]
      nodes_and_edges = usdm.to_neo4j_dict()
      filename = os.path.join("uploads", self.uuid, f"{self.uuid}.yaml")
      with open(f"{filename}", 'w') as f:
        f.write(yaml.dump(nodes_and_edges))

      self.set_status("running", "Converting data to database format", 10)
      ne = StudyFileNodesAndEdges(self.dir_path, nodes_and_edges)
      ne.dump()

      if self.upload_service.upper().startswith('GIT'):
        self.set_status("running", "Uploading to github", 15)
        git = GithubService()
        file_count = git.file_list(self.dir_path, "*.csv")
        for index in range(file_count):
          more = git.next()
          count = git.progress()
          percent = 15 + int(50.0 * (float(count) / float(file_count)))
          self.set_status("running", "Uploading to github", percent)
        git.load()
        files = git.upload_file_list(self.uuid)
      elif self.upload_service.upper().startswith('LOCAL'):
        self.set_status("running", "Uploading to github", 15)
        local = LocalService()
        file_count = local.file_list(self.dir_path, "*.csv")
        for index in range(file_count):
          more = local.next()
          count = local.progress()
          percent = 15 + int(50.0 * (float(count) / float(file_count)))
          self.set_status("running", "Uploading to local", percent)
        local.load()
        files = local.upload_file_list(self.uuid)
      else:
        self.set_status("running", "Uploading to dropbox", 15)
        dropbox = DropboxService()
        file_count = dropbox.file_list(self.dir_path, "*.csv")
        dropbox.upload()
        files = dropbox.upload_file_list(self.uuid)

      application_logger.info(f"Files: {files}") 
      self.set_status("running", "Loading database", 65)
      aura = AuraService()
      application_logger.debug(f"Aura load: {self.uuid} {files[0]}")
      aura.load(self.uuid, files)

      # Fix DM.RFICDTC to Informed Consent Obtained


      # Fix surrogates. Replace CDISC BC's with d4k
      self.set_status("running", "Fix Biomedical Concepts Surrogates", 70)

      self.set_status("running", "Fix Biomedical Concepts", 70)
      result = StudyDesignBC.fix(study_design.name)

      self.set_status("running", "Creating data contract", 75)
      name = study.name
      ns = RAService().namespace_by_name('d4k Study namespace')
      StudyDesignDataContract.create(name, ns['value'])

      self.set_status("running", "Adding SDTM domains", 80)
      result = StudyDesignSDTM.create(study_design.name)

      self.set_status("running", "Linking Biomedical Concepts", 90)
      result = StudyDesignBC.create(study_design.name)

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

  def _check_extension(self, filename):
    return True if filename.endswith('.xlsx') else False

  @staticmethod
  def _create(tx, self):
    query = """
      CREATE (df:StudyFile {filename: $filename, full_path: $full_path, uuid: $uuid, status: $status, stage: $stage, percentage: $percentage, service: $service, date_time: $date_time})
      RETURN df.uuid as uuid
    """
    results = tx.run(query, 
      filename=self.filename,
      full_path=self.full_path,
      uuid=self.uuid,
      stage=self.stage,
      status=self.status,
      percentage=self.percentage,
      service=self.service,
      date_time=self.date_time
    )
    for row in results:
      return row["uuid"]
    return None

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
