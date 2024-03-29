from typing import List
from model.base_node import BaseNode

class Variable(BaseNode):
  uri: str
  name: str
  code_list: str	
  code_list_uri: str	
  core: str
  data_type: str
  description: str
  label: str
  ordinal: int
  role: str
  value_domain: str
  crm_uri: str = ""