from d4kms_generic import application_logger
from d4kms_service import Neo4jConnection
from model.base_node import NodeId

class Element():

  def __init__(self, study_version, definition, name):
    self._study_version = study_version
    self._definition = definition
    self._name = name

  def definition(self):
    return self._definition
  
  def exists(self):
    return True
  
  def create(self):
    pass

  def read(self):
    if self._implemented():
      try:
        params = {'uuid': self._study_version.uuid}
        root = self._definition['root']
        item = self._definition['read']
        query = f"{root['query']} {item['query']}"
        #print(f"QUERY MERGE: {query}")
        result = self._read(query, params)
        if 'result' in result:
          return {'result': result['result']}
        else:
          return {'error': f"Failed to read {self._name}"}
      except Exception as e:
        return self._log_error("read", e)
    else:
      return self._not_implemented_result()
    
  def reference(self):
    if self._implemented():
      try:
        params = {'uuid': self._study_version.uuid}
        root = self._definition['root']
        item = self._definition['reference']
        query = f"{root['query']} {item['query']}"
        #print(f"QUERY MERGE: {query}")
        result = self._read(query, params)
        if 'result' in result:
          return {'result': {'instance': NodeId.wrap(result['result']), 'klass': item['klass'], 'attribute': item['attribute']}}
        else:
          return {'error': f"Failed to get reference for {self._name}"}
      except Exception as e:
        return self._log_error("reference", e)
    else:
      return self._not_implemented_result()

  def write(self, value):
    if self._implemented():
      try:
        params = self._build_params(value)
        root = self._definition['root']
        item = self._definition['write']
        query = f"{root['query']} {item['query']}"
        return self._write(query, params)
      except Exception as e:
        return self._log_error("write", e)
    else:
      return self._not_implemented_result()

  def _read(self, query, params):
    db = Neo4jConnection()
    with db.session() as session:
      #print(f"READ: {query}, {params}")  
      result = session.execute_read(self._read_method, query, params)
      if not result:
        return {'error': f"Failed to read {self._name}"}
      return {'result': result['value']}  

  def _write(self, query, params):
    db = Neo4jConnection()
    with db.session() as session:
      result = session.execute_write(self._write_method, query, params)
      if not result:
        return {'error': f"Failed to update {self._name}"}
      return {'result': result['value']}  

  def _log_error(self, operation, e):
    application_logger.exception(f"Exception raised during {operation} operation on element '{self._name}'", e)
    return {'error': f"Exception. Failed during {operation} operation on element '{self._name}'"}

  def _implemented(self):
    return True if self._definition['status'] == "ok" else False

  def _not_implemented_result(self):
    return {'result': f"Element '{self._name}' not implemented yet"}

  def _build_params(self, value):
    params = {}
    for param in self._definition['write']['data']:
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
