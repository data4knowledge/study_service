import logging
import traceback
from model.base_node import *
from d4kms_service import Neo4jConnection

class Element():

  method_map = {
    'full_title': {
      'status': "ok",
      'root': {
        'query': "MATCH (sv:StudyVersion {uuid: $uuid})-[]->(st:StudyTitle)-[]->(c:Code {code: 'C99905x1'})",
      },
      'read': {
        'query': "RETURN st.text as value",
      },
      'write': {
        'query': "SET st.text = $value RETURN st.text as value",
        'data': ['uuid', 'value']
      },
      'reference': {
        'instance': "RETURN st as value",
        'klass': 'StudyTitle', 
        'attribute': 'text'
      }
    },
    'trial_acronym': {
      'status': "ok",
      'root': {
        'query': "MATCH (sv:StudyVersion {uuid: $uuid})-[]->(st:StudyTitle)-[]->(c:Code {code: 'C94108'})",
      },
      'read': {
        'query': "RETURN st.text as value",
      },
      'write': {
        'query': "SET st.text = $value RETURN st.text as value",
        'data': ['uuid', 'value']
      },
      'reference': {
        'instance': "RETURN st as value",
        'klass': 'StudyTitle', 
        'attribute': 'text'
      }
    },
    'version_number': {
      'status': "ok",
      'root': {
        'query': "MATCH (sv:StudyVersion {uuid: $uuid})",
      },
      'read': {
        'query': "RETURN sv.versionIdentifier as value",
      },
      'write': {
        'query': "RETURN sv.versionIdentifier as value",
        'data': ['uuid', 'value']
      }
    },
    'sponsor_identifier': {
      'status': "ok",
      'root':{
        'query': "MATCH (sv:StudyVersion {uuid: $uuid})-[]->(si:StudyIdentifiers)-[]->(c:Code {code: 'C70793'})",
      },
      'read': {
        'query': "RETURN st.text as value",
      },
      'write': {
        'query': "SET st.text = $value RETURN st.text as value",
        'data': ['uuid', 'value']
      },
      'reference': {
        'instance': "RETURN si as value",
        'klass': 'StudyIdentifier', 
        'attribute': 'studyIdentifier'
      }
    },
  
    'study_phase': {'status': "no map"},
  #   phase = self._study_version.studyPhase.standardCode
  #   results = [{'instance': phase, 'klass': 'Code', 'attribute': 'decode'}]
  #   return self._set_of_references(results)
  
    'study_short_title': {'status': "no map"},
  #   results = [{'instance': self.protocol_document_version, 'klass': 'StudyProtocolDocumentVersion', 'attribute': 'briefTitle'}]
  #   return self._set_of_references(results)

    'study_full_title': {'status': "no map"},
  #   #results = [{'instance': self.protocol_document_version, 'klass': 'StudyProtocolDocumentVersion', 'attribute': 'officialTitle'}]
  #   result = Element( self._study_version, 'full_title').reference()
  #   print(f"RESULT: {result}")
  #   refs = [result['result']] if 'result' in result else []
  #   return self._set_of_references(refs)

    'study_acronym': {'status': "no map"},
  #   results = [{'instance': self._study_version, 'klass': 'StudyVersion', 'attribute': 'studyAcronym'}]
  #   return self._set_of_references(results)

    'study_version_identifier': {'status': "no map"},
  #   results = [{'instance': self._study_version, 'klass': 'StudyVersion', 'attribute': 'versionIdentifier'}]
  #   return self._set_of_references(results)

    'study_identifier': {'status': "no map"},
  #   identifier = self._sponsor_identifier()
  #   results = [{'instance': identifier, 'klass': 'StudyIdentifier', 'attribute': 'studyIdentifier'}]
  #   return self._set_of_references(results)

    'study_regulatory_identifiers': {'status': "no map"},
  #   results = []
  #   identifiers = self._study_version.studyIdentifiers
  #   for identifier in identifiers:
  #     if identifier.studyIdentifierScope.type.code == 'C188863' or identifier.studyIdentifierScope.type.code == 'C93453':
  #       item = {'instance': identifier, 'klass': 'StudyIdentifier', 'attribute': 'studyIdentifier'}
  #       results.append(item)
  #   return self._set_of_references(results)

     'study_date': {'status': "no map"},
  #   dates = self.protocol_document_version.dateValues
  #   for date in dates:
  #     if date.type.code == 'C99903x1':
  #       results = [{'instance': date, 'klass': 'GovernanceDate', 'attribute': 'dateValue'}]
  #       return self._set_of_references(results)
  #   return None
  
    'approval_date': {'status': "no map"},
  #   dates = self._study_version.dateValues
  #   for date in dates:
  #     if date.type.code == 'C132352':
  #       results = [{'instance': date, 'klass': 'GovernanceDate', 'attribute': 'dateValue'}]
  #       return self._set_of_references(results)
  #   return None

    'organization_name_and_address': {'status': "no map"},
  #   identifier = self._sponsor_identifier()
  #   results = [
  #     {'instance': identifier.studyIdentifierScope, 'klass': 'Organization', 'attribute': 'name'},
  #     {'instance': identifier.studyIdentifierScope.legalAddress, 'klass': 'Address', 'attribute': 'text'},
  #   ]
  #   return self._set_of_references(results)

    'amendment': {'status': "no map"},
  #   amendments = self._study_version.amendments
  #   results = [{'instance': amendments[-1], 'klass': 'StudyAmendment', 'attribute': 'number'}]
  #   return self._set_of_references(results)

    'amendment_scopes': {'status': "no map"},
  #   results = []
  #   amendment = self._study_version.amendments[-1]
  #   for item in amendment.enrollments:
  #     if item.type.code == "C68846":
  #       results = [{'instance': item.type, 'klass': 'Code', 'attribute': 'decode'}]
  #       return self._set_of_references(results)
  #     else:
  #       entry = {'instance': item.code.standardCode, 'klass': 'Code', 'attribute': 'decode'}
  #       results.append(entry)
  #   return self._set_of_references(results)
    
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
        root = self.method_map[self._element]['root']
        item = self.method_map[self._element]['read']
        query = f"{root['query']} {item['query']}"
        result = self._read(query, params)
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
        root = self.method_map[self._element]['root']
        item = self.method_map[self._element]['reference']
        query = f"{root['query']} {item['query']}"
        result = self._read(query, params)
        if 'result' in result:
          return {'result': {'instance': NodeId.wrap(result['result']), 'klass': item['klass'], 'attribute': item['attribute']}}
        else:
          return {'error': f"Failed to get reference for {self._element}"}
      except Exception as e:
        return self._log_error("reference", e)
    else:
      return {'error': f"No implementation for element {self._element} yet"}

  def write(self, value):
    if self._implemented():
      try:
        params = self._build_params(value)
        root = self.method_map[self._element]['root']
        item = self.method_map[self._element]['write']
        query = f"{root['query']} {item['query']}"
        return self._write(query, params)
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
