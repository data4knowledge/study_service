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
from model.utility.configuration import ConfigurationNode
from model.data_file import DataFile

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
          db = Neo4jConnection()
          with db.session() as session:
            session.execute_write(self._create, self)
          db.close()
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

      self.set_status("running", "Uploading to local", 15)
      local = LocalService()
      file_count = local.file_list(self.dir_path, "*.csv")
      for index in range(file_count):
        more = local.next()
        count = local.progress()
        percent = 15 + int(50.0 * (float(count) / float(file_count)))
        # self.set_status("running", "Uploading to local", percent)
      local.load()
      files = local.upload_file_list(self.uuid)

      # application_logger.info(f"Files: {files}") 
      self.set_status("running", "Loading database", 20)
      aura = AuraService()
      application_logger.debug(f"Aura load: {self.uuid} {files[0]}")
      aura.load(self.uuid, files)

      # Fix DM.RFICDTC to Informed Consent Obtained


      # Fix surrogates. Replace CDISC BC's with d4k
      self.set_status("running", "Fix Biomedical Concepts Surrogates", 30)
      result = StudyDesignBC.make_dob_surrogate_as_bc(study_design_uuid)
      result = StudyDesignBC.pretty_properties_for_bc(study_design_uuid)

      self.set_status("running", "Fix Biomedical Concepts", 40)
      result = StudyDesignBC.fix(study_design_uuid)

      self.set_status("running", "Creating data contract", 50)
      name = study.name
      ns = RAService().namespace_by_name('d4k Study namespace')
      StudyDesignDataContract.create(name, ns['value'])

      self.set_status("running", "Adding SDTM domains", 60)
      result = StudyDesignSDTM.create(study_design_uuid)

      # Add permissable SDTM variables
      self.set_status("running", "Add permissible SDTM variables", 70)
      result = StudyDesignSDTM.add_permissible_sdtm_variables(study_design_uuid)

      # Add missing links to CRM
      self.set_status("running", "Link BRTHDTC to CRM", 75)
      result = StudyDesignBC.fix_links_to_crm(study_design_uuid)

      # Add missing BC links to SDTM (Probably superfluous. E.g. DS does not have a link to BC Exposure, but it shows Exposure information if configured)
      # self.set_status("running", "Link BC to SDTM", 89)
      # result = StudyDesignSDTM.add_links_to_sdtm(study_design.name)

      self.set_status("running", "Linking Biomedical Concepts", 80)
      result = StudyDesignBC.create(study_design_uuid)

      # Fix BC name/label
      self.set_status("running", "Fix BC name/label", 85)
      result = StudyDesignBC.fix_bc_name_label(study_design_uuid)

      self.set_status("running", "Create default configuration", 90)
      ConfigurationNode.create_default_configuration()

      self.set_status("running", "Add properties to CT", 90)
      DataFile.add_properties_to_ct(study_design_uuid)

      self.set_status("complete", "Finished", 100)
      return True

    except Exception as e:
      self.error = f"Failed to process and load study. Was {self.status}"
      self._log(e, f"{traceback.format_exc()}")
      return False

  def set_status(self, status, stage, percentage):
    self.status = status
    application_logger.info(f"Study load, status: {status} {stage}")
    #print(f"Study load, status: {status}")
    db = Neo4jConnection()
    with db.session() as session:
      session.execute_write(self._set_status, self.uuid, status, stage, percentage)
    db.close()

  def get_status(self):
    db = Neo4jConnection()
    with db.session() as session:
      query = "MATCH (n:StudyFile {uuid: '%s'}) RETURN n.status as status, n.percentage as percent, n.stage as stage" % (self.uuid)
      result = session.run(query)
      status = ""
      for record in result:
        #print(f"RECORD: {record}")
        status = {'status': record['status'], 'percentage': record['percent'], 'stage': record['stage'] }
    db.close()
    return status
    
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
