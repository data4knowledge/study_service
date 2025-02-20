from typing import List, Union
from uuid import uuid4
from d4kms_service import Neo4jConnection
from d4kms_generic import application_logger
from model.domain import Domain
from model.variable import Variable
from service.bc_service import BCService
from service.sdtm_service import SDTMService
from model.crm import CRMNode
from model.biomedical_concept import BiomedicalConceptSimple
from d4kms_generic import application_logger
from model.study_design_bc import StudyDesignBC
from model.utility.configuration import ConfigurationNode, Configuration

import json
import copy
import traceback

# from model.utility.define_queries import define_vlm_query, crm_link_query, _add_missing_links_to_crm_query, study_info_query, domains_query, domain_variables_query, variables_crm_link_query, define_codelist_query, define_test_codes_query, find_ct_query
from datetime import datetime
# from pathlib import Path

class StudyForm():
  define_xml: str
  configuration: dict = {}

  @classmethod
  def make_form(self, uuid, page, size, filter):
    self._get_configuration(self)
    bcs = StudyDesignBC.get_bcs_and_properties(uuid)
    visits = StudyDesignBC.get_visits(uuid)
    first = next(iter(bcs))
    print(first)
    result = {'items': bcs, 'visits': visits, 'page': page, 'size': size, 'filter': filter, 'count': 1 }
    return result

  @classmethod
  def make_lab_transfer_spec(self, uuid, page, size, filter):
    bcs = StudyDesignBC.get_lab_transfer_spec(uuid)
    result = {'items': bcs, 'page': page, 'size': size, 'filter': filter, 'count': 1 }
    return result

  @classmethod
  def datapoint_form(self, datapoint, page, size, filter):
    self._get_configuration(self)
    bcs = StudyDesignBC.get_datapoint_stuff(datapoint)
    valid_values = StudyDesignBC.get_datapoint_valid_values(datapoint)
    result = {'items': bcs, 'source_datapoint': datapoint, 'valid_values': valid_values, 'visits': [], 'page': page, 'size': size, 'filter': filter, 'count': 1 }
    return result

  @staticmethod
  def _get_configuration(self):
    configuration = ConfigurationNode.get()
    # Check that configuration existed
    if configuration.uuid == "failed":
      print("Configuration does not exist. Creating with default parameters")
      ConfigurationNode.create_default_configuration()
    
    self.configuration = configuration
