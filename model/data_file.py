import os
from .base_node import BaseNode
from d4kms_service import Neo4jConnection
from d4kms_generic import application_logger
from d4kms_generic import ServiceEnvironment
from uuid import uuid4
from service.github_service import GithubService
from service.aura_service import AuraService
from service.local_service import LocalService
from model.utility.raw_data import import_raw_data
from service.ct_service import CTService

class DataFile(BaseNode):
  uuid: str = ""
  filename: str = ""
  full_path: str = ""
  dir_path: str = ""
  status: str = ""
  error: str = ""
  data_type: str = ""
  upload_service: str = None

  def __init__(self, *a, **kw):
    super().__init__(*a, **kw)
    se = ServiceEnvironment()
    self.upload_service = se.get('UPLOAD_SERVICE')

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
      if self.upload_service.upper().startswith('GIT'):
        self.set_status("running", "Uploading to github", 15)
        git = GithubService()
        file_count = git.file_list(self.dir_path, self.filename)
        for index in range(file_count):
          more = git.next()
          count = git.progress()
          percent = 15 + int(65.0 * (float(count) / float(file_count)))
          self.set_status("running", "Uploading to github", percent)
        git.load()
      elif self.upload_service.upper().startswith('LOCAL'):
        self.set_status("running", "Uploading to github", 15)
        local = LocalService()
        file_count = local.file_list(self.dir_path, "*.csv")
        for index in range(file_count):
          more = local.next()
          count = local.progress()
          percent = 15 + int(65.0 * (float(count) / float(file_count)))
        self.set_status("running", "Uploading to local neo4j", percent)
        local.load()
        files = local.upload_file_list(self.uuid)

      self.set_status("running", "Loading database", 80)
      aura = AuraService()
      application_logger.debug(f"Aura load: {self.uuid} {files[0]}")
      if self.data_type == 'identifier':
        try:
          aura.load_identifiers(files[0]['file_path'])
        except Exception as e:
          self.error = f"Couldn't load file"
          application_logger.exception(self.error, e)
          return False
      elif self.data_type == 'subject': 
        try:
          aura.load_datapoints(files[0]['file_path'])
        except Exception as e:
          self.error = f"Couldn't load file"
          application_logger.exception(self.error, e)
          return False
      elif self.data_type == 'raw_data': 
        try:
          print("Processing raw data to identifiers and datapoints")
          import_files = import_raw_data(self.dir_path, self.filename)
          url_path = files[0]['file_path'].rsplit("/",1)[0]
          aura.load_identifiers(os.path.join(url_path, import_files['identifiers']))
          aura.load_datapoints(os.path.join(url_path, import_files['datapoints']))
        except Exception as e:
          self.error = f"Couldn't load file"
          application_logger.exception(self.error, e)
          return False
      else:
        print("DataFile upload: Unknown data_type")
        application_logger.info("DataFile upload: Unknown data_type")

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

  @classmethod
  def add_properties_to_ct(cls):
    results = cls._add_properties_to_ct()
    application_logger.info("Added properties to CT")
    return results

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

  @staticmethod
  def _add_properties_to_ct():
    ct = CTService()
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign {name: '%s'})-[:BIOMEDICAL_CONCEPTS_REL]->(bc:BiomedicalConcept)-[]->(bcp:BiomedicalConceptProperty)-[]->(rc:ResponseCode)-[]->(c:Code)
        WHERE c.updated is null
        RETURN DISTINCT c.code as code
      """ % ("Study Design 1")
      # print("query",query)
      results = session.run(query)
      codes_to_update = [it['code'] for it in results.data()]
      if  len(codes_to_update) == 0:
        print("No need to update:", len(codes_to_update))
        return {'Message': "No need to update"}
      print("CT codes to fix:", len(codes_to_update))
      items = []
      count = 0
      for code in codes_to_update:
        print("code",code)
        response = ct.find_by_identifier(code)
        first = response[0] if len(response) > 0 else None
        if first:
          items.append({'code': code, 'name': first['child']['name'], 'notation': first['child']['notation'], 'pref_label': first['child']['pref_label']})
      for item in items:
        query = """
          MATCH (c:Code {code: '%s'})
          SET c.pref_label = '%s'
          SET c.notation = '%s'
          SET c.updated = True
          RETURN count(c) as count
        """ % (item['code'], item['pref_label'], item['notation'])
        print("query",query)
        results = session.run(query)
        count = count + int(results.data()[0]['count'])
    return {'count': count}
