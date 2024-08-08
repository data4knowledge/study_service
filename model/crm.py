import os
from model.base_node import BaseNode
from model.utility.utility import read_yaml_file
from d4kms_generic import application_logger
from d4kms_service import Neo4jConnection
from service.crm_service import CRMService
from uuid import uuid4

class CRMNode(BaseNode):
  crm_uuid: str = ""
  uri: str = ""
  sdtm: str = ""
  datatype: str = ""

class CRM():

  DIR = 'data'
  FILENAME = 'crm.yaml'

  def __init__(self):
    self._definitions = read_yaml_file(os.path.join(self.DIR, self.FILENAME))

  def execute(self):
    try:
      nodes = {}
      crm_service = CRMService()
      for entry in self._definitions['variables']:
        for ref in entry['canonical']:
          result = crm_service.leaf(ref['node'], ref['data_type'], ref['property'])
          uri = result['uri']
          if uri in nodes:
            nodes[uri]['sdtm'].append(entry['name'])
          else: 
            nodes[uri] = {'uuid': str(uuid4()), 'crm_uuid': result['uuid'], 'uri': result['uri'], 'datatype': ref['data_type'], 'sdtm': [entry['name']]}
      for k, v in nodes.items():
        v['sdtm'] = ",".join(v['sdtm'])
        CRMNode.create(v)
        application_logger.info(f"Created CRM node {v}")
      return True
    except Exception as e:
      application_logger.exception(f"Exception raised while creating CRM nodes", e)
      return False

  def add_crm_nodes(self):
    try:
      application_logger.info(f"N.B! Creating extra CRM nodes")
      crm_vars = [
        {'sdtm': '--DOSFRQ', 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/frequency/coding/code', 'datatype': 'coding'}
       ,{'sdtm': '--ROUTE' , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/route/coding/code', 'datatype': 'coding'}
       ,{'sdtm': '--TRT'   , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/description/coding/code', 'datatype': 'coding'}
       ,{'sdtm': '--DOSFRM', 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/form/coding/code', 'datatype': 'coding'}
       ,{'sdtm': '--DOSE'  , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/single_dose/quantity/value', 'datatype': 'quantity'}
       ,{'sdtm': '--DOSU'  , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/single_dose/quantity/unit', 'datatype': 'quantity'}
       ,{'sdtm': '--STDTC' , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'uri': 'https://crm.d4k.dk/dataset/common/period/period_start/date_time/value', 'datatype': 'date_type'}
       ,{'sdtm': '--ENDTC' , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'uri': 'https://crm.d4k.dk/dataset/common/period/period_start/date_time/value', 'datatype': 'date_type'}
      ]
      for node in crm_vars:
        CRMNode.create(node)
        application_logger.info(f"Created CRM node {node}")
      return True
    except Exception as e:
      application_logger.exception(f"Exception raised while creating extra CRM nodes", e)
      return False

