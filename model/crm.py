import os
from model.base_node import BaseNode
from model.utility.utility import read_yaml_file
from d4kms_generic import application_logger
from service.crm_service import CRMService
from uuid import uuid4

class CRMNode(BaseNode):
  crm_uuid: str = ""
  uri: str = ""
  sdtm: str = ""

class CRM():

  DIR = 'data'
  FILENAME = 'crm.yaml'

  def __init__(self):
    self._definitions = read_yaml_file(os.path.join(self.DIR, self.FILENAME))

  def execute(self):
    try:
      crm_service = CRMService()
      for entry in self._definitions['variables']:
        for ref in entry['canonical']:
          result = crm_service.leaf(ref['node'], ref['data_type'], ref['property'])
          application_logger.info(f"CRM result {result} for {entry['name']}, {ref}")
          CRMNode.create({'uuid': str(uuid4()), 'crm_uuid': result['uuid'] , 'uri': result['uri'], 'sdtm': entry['name']})
      return True
    except Exception as e:
      application_logger.exception(f"Exception raised while creating CRM nodes", e)
      return False
