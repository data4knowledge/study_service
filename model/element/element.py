import logging
import traceback
from model.neo4j_connection import Neo4jConnection

class Element():

  method_map = {
    'full_title': {
      'read': {
        'query': "MATCH (sv:StudyVersion {uuid: $uuid}) RETURN sv.studyTitle",
      },
      'write': {
        'query': "MATCH (sv:StudyVersion {uuid: $uuid}) SET sv.studyTitle = $value RETURN sv as study_version",
        'data': ['uuid', 'value']
      }
    }
  }

  def __init__(self, study_version, element):
    self._study_version = study_version
    self._element = element

  def exists(self):
    return True
  
  def create(self):
    pass

  def read(self):
    try:
      db = Neo4jConnection()
      result = db.query(self.method_map[self._element]['read']['query'])
      return {'result': result}
    except Exception as e:
      self._log_error("read", e)

  def write(self, value):
    try:
      params = self._build_params(value)
      return self._write(self.method_map[self._element]['write']['query'], params)
    except Exception as e:
      self._log_error("write", e)

  def _write(self, query, params):
    db = Neo4jConnection()
    with db.session() as session:
      result = session.execute_write(self._write_method, query, params)
      if not result:
        return {'error': "Failed to update {self._element}"}
      return {'result': params['value']}  

  def _log_error(self, operation, e):
    logging.error(f"Exception raised during {operation} operation on element '{self._element}'")
    logging.error(f"Exception {e}\n{traceback.format_exc()}")
    return {'error': f"Exception. Failed during {operation} operation on element '{self._element}'"}

  def _build_params(self, value):
    params = {}
    for param in self.method_map[self._element]['write']['data']:
      if param == 'uuid':
        params[param] = self._study_version.uuid
      elif param == 'value':
        params[param] = value
    return params
  
  @staticmethod
  def _write_method(tx, query, params):
    result = tx.run(query, params)
    for row in result:
      return True
    return None
