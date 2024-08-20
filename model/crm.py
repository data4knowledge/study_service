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
        {'sdtm': '--DOSFRQ'  , 'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/frequency/coding/code'     , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'datatype': 'coding'}
        ,{'sdtm': '--ROUTE'  , 'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/route/coding/code'         , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'datatype': 'coding'}
        ,{'sdtm': '--TRT'    , 'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/description/coding/code'   , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'datatype': 'coding'}
        ,{'sdtm': '--DOSFRM' , 'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/form/coding/code'          , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'datatype': 'coding'}
        ,{'sdtm': '--DOSE'   , 'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/single_dose/quantity/value', 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'datatype': 'quantity'}
        ,{'sdtm': '--DOSU'   , 'uri': 'https://crm.d4k.dk/dataset/therapeutic_intervention/single_dose/quantity/unit' , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'datatype': 'quantity'}
        ,{'sdtm': '--STDTC'  , 'uri': 'https://crm.d4k.dk/dataset/common/period/period_start/date_time/value'         , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'datatype': 'date_type'}
        ,{'sdtm': '--ENDTC'  , 'uri': 'https://crm.d4k.dk/dataset/common/period/period_end/date_time/value'           , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'datatype': 'date_type'}
        ,{'sdtm': 'AERLDEV'  , 'uri': 'https://crm.d4k.dk/dataset/adverse_event/causality/device'                     , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'datatype': 'coding'}
        ,{'sdtm': 'AERELNST' , 'uri': 'https://crm.d4k.dk/dataset/adverse_event/causality/non_study_treatment'        , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'datatype': 'coding'}
        ,{'sdtm': 'AEREL'    , 'uri': 'https://crm.d4k.dk/dataset/adverse_event/causality/related'                    , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'datatype': 'coding'}
        ,{'sdtm': 'AEACNDEV' , 'uri': 'https://crm.d4k.dk/dataset/adverse_event/response/concomitant_treatment'       , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'datatype': 'coding'}
        ,{'sdtm': 'AEACNOTH' , 'uri': 'https://crm.d4k.dk/dataset/adverse_event/response/other'                       , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'datatype': 'coding'}
        ,{'sdtm': 'AEACN'    , 'uri': 'https://crm.d4k.dk/dataset/adverse_event/response/study_treatment'             , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'datatype': 'coding'}
        ,{'sdtm': 'AESER'    , 'uri': 'https://crm.d4k.dk/dataset/adverse_event/serious'                              , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'datatype': 'coding'}
        ,{'sdtm': 'AESEV'    , 'uri': 'https://crm.d4k.dk/dataset/adverse_event/severity'                             , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'datatype': 'coding'}
        ,{'sdtm': 'AETERM'   , 'uri': 'https://crm.d4k.dk/dataset/adverse_event/term'                                 , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'datatype': 'coding'}
        ,{'sdtm': 'AETOXGR'  , 'uri': 'https://crm.d4k.dk/dataset/adverse_event/toxicity/grade'                       , 'uuid': str(uuid4()), 'crm_uuid': 'tbd', 'datatype': 'coding'}
      ]
      for node in crm_vars:
        CRMNode.create(node)
        application_logger.info(f"Created CRM node {node}")
      return True
    except Exception as e:
      application_logger.exception(f"Exception raised while creating extra CRM nodes", e)
      return False

