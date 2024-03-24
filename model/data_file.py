import os
from .base_node import BaseNode
from d4kms_service import Neo4jConnection
from d4kms_generic import application_logger
from uuid import uuid4

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
      import time
      for count in range(0, 100, 10):
        self.set_status("running", "Processing CSV file", count)
        time.sleep(1)
      # -----------

      self.set_status("complete", "Finished", 100)
      return True
    except Exception as e:
      self.error = f"Failed to process and load study. Was {self.status}"
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
