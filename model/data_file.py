import os
from .base_node import BaseNode
from d4kms_service import Neo4jConnection
from d4kms_generic import application_logger
from d4kms_generic import ServiceEnvironment
from uuid import uuid4
from service.github_service import GithubService
from service.aura_service import AuraService

class DataFile(BaseNode):
  uuid: str = ""
  filename: str = ""
  full_path: str = ""
  dir_path: str = ""
  status: str = ""
  error: str = ""
  data_type: str = ""

  def create(self, filename, contents, data_type):
    if not self._check_extension(filename):
      self.error = "Invalid extension, must be '.csv'"
      return False
    self.filename = filename
    self.uuid = str(uuid4())
    self.dir_path = os.path.join("uploads", self.uuid)
    self.full_path = os.path.join("uploads", self.uuid, filename)
    self.data_type = data_type
    db = Neo4jConnection()
    with db.session() as session:
      self.set_status("commencing", "Uploading file", 0)
      try:
        os.mkdir(self.dir_path)
      except Exception as e:
        self.error = f"Failed to create directory"
        application_logger.exception(self.error, e)
        return False
      else:
        try:
          with open(self.full_path, 'wb') as f:
            f.write(contents)
        except Exception as e:
          self.error = f"Failed to save file content"
          application_logger.exception(self.error, e)
          return False
        else:
          try:
            session.execute_write(self._create, self.filename, self.full_path, self.uuid)
          except Exception as e:
            self.error = f"Failed to save file details"
            application_logger.exception(self.error, e)
            return False
          else:
            self.set_status("initialised", "Uploaded file", 0)
            return True

  def execute(self):
    try:
      self.set_status("running", "Processing CSV file", 0)      
      # -----------
      # Temporary code in place of actual processing
      # Code shpould process csv file depending on self.data_type ['subject' or 'identifier']
      # 'subject = subject data
      # 'identifier' = site and subject identifiers data
      # import time
      # for count in range(0, 20, 10):
      #   print("loading ----",self.filename,self.full_path)
      #   self.set_status("running", "Processing CSV file", count)
      #   time.sleep(1)
      # -----------
      self.set_status("running", "Uploading to github", 15)
      git = GithubService()
      print("self.dir_path",self.dir_path)
      file_count = git.file_list(self.dir_path, self.filename)
      print("file_count",file_count)
      print("git.files",git.files)
      for index in range(file_count):
        print("index",index)
        more = git.next()
        count = git.progress()
        percent = 15 + int(65.0 * (float(count) / float(file_count)))
        self.set_status("running", "Uploading to github", percent)
      git.load()

      # self.set_status("running", "Loading database", 80)
      aura = AuraService()
      files = git.upload_file_list()
      print("files",files)
      sv = ServiceEnvironment()
      project_root = sv.get("GITHUB_BASE")
      print("project_root",project_root)
      for git_filename in files:
        print("git_filename",git_filename)
        file_path = os.path.join(project_root, self.dir_path, git_filename)
        print("file_path",file_path)

        if self.data_type == 'identifier':
          try:
            aura.load_identifiers(self.dir_path,git_filename)
            # session.execute_write(self._load_identifiers, file_path)
          except Exception as e:
            self.error = f"Couldn't load file"
            application_logger.exception(self.error, e)
            return False
        # elif self.data_type == 'subject': 
        #   try:
        #     session.execute_write(self._load_data, file_path)
        #   except Exception as e:
        #     self.error = f"Couldn't load file"
        #     application_logger.exception(self.error, e)
        #     return False
        else:
          print("DataFile.execute: Unknown data_type")

      self.set_status("complete", "Finished", 100)
      return True
    except Exception as e:
      self.error = f"Failed to process and load datapoints/identifiers. Was {self.status}"
      application_logger.exception(self.error, e)
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
      query = "MATCH (n:DataFile {uuid: '%s'}) RETURN n.status as status, n.percentage as percent, n.stage as stage" % (self.uuid)
      result = session.run(query)
      for record in result:
        #print(f"RECORD: {record}")
        return {'status': record['status'], 'percentage': record['percent'], 'stage': record['stage'] }
    return ""

  @staticmethod
  def _get_import_directory(tx):
    query = "call dbms.listConfig()"
    results = tx.run(query)
    config = [x.data() for x in results]
    import_directory = next((item for item in config if item["name"] == 'server.directories.import'), None)
    return import_directory['value']
    
  @staticmethod
  def _copy_file_to_db_import(source, target_folder):
    assert os.path.exists(target_folder), f"Neo4j db import directory not found: {target_folder}"
    with open(source,'r') as f:
        txt = f.read()
    target_file = os.path.join(target_folder,os.path.basename(source))
    with open(target_file,'w') as f:
        f.write(txt)
    print("Written",target_file)

  @staticmethod
  def _load_identifiers(tx, filename):
    print("Going to import subject and site with filename:",filename)
    query = f"""
        LOAD CSV WITH HEADERS FROM 'file:///{filename}' AS site_row
        MATCH (design:StudyDesign {{name:'Study Design 1'}})
        MERGE (s:Subject {{identifier:site_row['USUBJID']}})
        MERGE (site:StudySite {{name:site_row['SITEID']}})
        MERGE (s)-[:ENROLLED_AT_SITE_REL]->(site)
        MERGE (site)<-[:MANAGES_SITE]-(researchOrg)
        MERGE (researchOrg)<-[:ORGANIZATIONS_REL]-(design)
        RETURN count(*)
    """
    print("query",query)
    results = tx.run(query)
    data = [x.data() for x in results]
    print("---data",data)
    return None

  @staticmethod
  def _load_data(tx, filename):
    print("Going to import data with filename:",filename)
    query = f"""
        LOAD CSV WITH HEADERS FROM 'file:///{filename}' AS data_row
        MATCH (dc:DataContract {{uri:data_row['DC_URI']}})
        MATCH (design:StudyDesign {{name:'Study Design 1'}})
        MERGE (d:DataPoint {{uri: data_row['DATAPOINT_URI'], value: data_row['VALUE']}})
        MERGE (s:Subject {{identifier:data_row['USUBJID']}})
        MERGE (dc)<-[:FOR_DC_REL]-(d)
        MERGE (d)-[:FOR_SUBJECT_REL]->(s)
        RETURN count(*)
    """
    results = tx.run(query)
    data = [x.data() for x in results]
    print("---data",data)
    return None

  @staticmethod
  def _create(tx, filename, full_path, uuid):
    query = """
      CREATE (n:DataFile {filename: $filename, full_path: $full_path, uuid: $uuid, status: 'Initialised'})
      RETURN n.uuid as uuid
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
    return True if filename.endswith('.csv') else False

  @staticmethod
  def _set_status(tx, uuid, status, stage, percentage):
    query = """
      MATCH (n:DataFile {uuid: $uuid})
      SET n.status = $status, n.stage = $stage, n.percentage = $percentage
      RETURN n
    """
    results = tx.run(query, uuid=uuid, status=status, stage=stage, percentage=percentage)
    for row in results:
      return DataFile.wrap(row['n'])
    return None
