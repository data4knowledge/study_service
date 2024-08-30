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

import json
import copy
import traceback

# from model.utility.define_queries import define_vlm_query, crm_link_query, _add_missing_links_to_crm_query, study_info_query, domains_query, domain_variables_query, variables_crm_link_query, define_codelist_query, define_test_codes_query, find_ct_query
from datetime import datetime
# from pathlib import Path

class StudyCrf():
  define_xml: str

  @classmethod
  def make_crf(cls, uuid, page, size, filter):
    result = {'items': [], 'page': page, 'size': size, 'filter': filter, 'count': 1 }
    return result
