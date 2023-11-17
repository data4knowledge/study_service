from .node import Node
from .neo4j_connection import Neo4jConnection
from uuid import uuid4

import os

class StudyFile(Node):
  uuid: str
  full_path: str
  status: str

  @classmethod
  def create(cls, filename, contents):
    if not cls._check_extension(filename):
      return None, "Invalid extension, must be '.xlsx'"
    db = Neo4jConnection()
    uuid = str(uuid4())
    dir_path = os.path.join("uploads", uuid)
    full_path = os.path.join("uploads", uuid, filename)
    with db.session() as session:
      try:
        os.mkdir(dir_path)
      except Exception as e:
        return None, f"Failed to create directory {e}"
      else:
        try:
          with open(full_path, 'wb') as f:
            f.write(contents)
        except Exception as e:
          return None, f"Failed to save file content {e}"
        else:
          try:
            session.execute_write(cls._create, full_path, uuid)
          except Exception as e:
            return None, f"Failed to save file details {e}"
          else:    
            return uuid, ""

  def clear(self):
    db = Neo4jConnection()
    with db.session() as session:
      status = session.execute_write(self._set_status, self.uuid, 'initialised')

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
  def _create(tx, full_path, uuid):
    query = """
      CREATE (df:StudyFile {full_path: $full_path, uuid: $uuid, status: 'initialised'})
      RETURN df.uuid as uuid
    """
    results = tx.run(query, 
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

