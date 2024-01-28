import logging
import traceback
from d4kms_service import Neo4jConnection

class Element():

  method_map = {
    'full_title': {
      'status': "ok",
      'read': {
        'query': "MATCH (sv:StudyVersion {uuid: $uuid})-[]->(st:StudyTitle)-[]->(c:Code {code: 'C99905x1'}) RETURN st.text as value",
      },
      'write': {
        'query': "MATCH (sv:StudyVersion {uuid: $uuid})-[]->(st:StudyTitle)-[]->(c:Code {code: 'C99905x1'}) SET st.text = $value RETURN st.text as value",
        'data': ['uuid', 'value']
      },
      'reference': {
        'instance': "MATCH (sv:StudyVersion {uuid: $uuid})-[]->(st:StudyTitle)-[]->(c:Code {code: 'C99905x1'}) RETURN st as value",
        'klass': 'StudyProtocolDocumentVersion', 
        'attribute': 'officialTitle'
      }
    },
    'trial_acronym': {
      'status': "ok",
      'read': {
        'query': "MATCH (sv:StudyVersion {uuid: $uuid})-[]->(st:StudyTitle)-[]->(c:Code {code: 'C94108'}) RETURN sv.text as value",
      },
      'write': {
        'query': "MATCH (sv:StudyVersion {uuid: $uuid})-[]->(st:StudyTitle)-[]->(c:Code {code: 'C94108'}) SET sv.text = $value RETURN sv.text as value",
        'data': ['uuid', 'value']
      }
    },
    'version_number': {
      'status': "ok",
      'read': {
        'query': "MATCH (sv:StudyVersion {uuid: $uuid}) RETURN sv.versionIdentifier as value",
      },
      'write': {
        'query': "MATCH (sv:StudyVersion {uuid: $uuid}) SET sv.versionIdentifier = $value RETURN sv.versionIdentifier as value",
        'data': ['uuid', 'value']
      }
    },
    'compound_code': {
      'status': "no map"
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
    if self._implemented():
      try:
        params = {'uuid': self._study_version.uuid}
        result = self._read(self.method_map[self._element]['read']['query'], params)
#        print(f"QUERY: {result}")
        if 'result' in result:
          return {'result': result['result']}
        else:
          return {'error': f"Failed to read {self._element}"}
      except Exception as e:
        return self._log_error("read", e)
    else:
      return self._implemented_status()

  def reference(self):
    if self._implemented():
      try:
        params = {'uuid': self._study_version.uuid}
        item = self.method_map[self._element]['reference']
        result = self._read(item['instance'], params)
        if 'result' in result:
          return {'result': {'instance': result['result'], 'klass': item['klass'], 'attribute': item['attribute']}}
        else:
          return {'error': f"Failed to get reference for {self._element}"}
      except Exception as e:
        return self._log_error("reference", e)
    else:
      return self._implemented_status()

  def write(self, value):
    if self._implemented():
      try:
        params = self._build_params(value)
        return self._write(self.method_map[self._element]['write']['query'], params)
      except Exception as e:
        return self._log_error("write", e)
    else:
      return self._implemented_status()

  def _read(self, query, params):
    db = Neo4jConnection()
    with db.session() as session:
      print(f"READ: {query}, {params}")  
      result = session.execute_read(self._read_method, query, params)
      if not result:
        return {'error': f"Failed to read {self._element}"}
      return {'result': result['value']}  

  def _write(self, query, params):
    db = Neo4jConnection()
    with db.session() as session:
      result = session.execute_write(self._write_method, query, params)
      if not result:
        return {'error': f"Failed to update {self._element}"}
      return {'result': result['value']}  

  def _log_error(self, operation, e):
    logging.error(f"Exception raised during {operation} operation on element '{self._element}'")
    logging.error(f"Exception {e}\n{traceback.format_exc()}")
    return {'error': f"Exception. Failed during {operation} operation on element '{self._element}'"}

  def _implemented(self):
    return True if self._element in self.method_map and self.method_map[self._element]['status'] == "ok" else False

  def _implemented_status(self):
    if self._element in self.method_map:
      return {'result': f"{self.method_map[self._element]['status']}"}
    else:
      return {'result': f"not configured"}

  def _build_params(self, value):
    params = {}
    for param in self.method_map[self._element]['write']['data']:
      if param == 'uuid':
        params[param] = self._study_version.uuid
      elif param == 'value':
        params[param] = value
    return params
  
  @staticmethod
  def _read_method(tx, query, params):
    result = tx.run(query, params)
    for row in result:
      return row
    return None

  @staticmethod
  def _write_method(tx, query, params):
    result = tx.run(query, params)
    for row in result:
      return row
    return None
