from typing import List, Literal
from .base_node import *
from .alias_code import AliasCode
from .response_code import ResponseCode

class BiomedicalConceptProperty(NodeNameLabel):
  isRequired: bool
  isEnabled: bool
  datatype: str
  responseCodes: List[ResponseCode] = []
  code: AliasCode
  instanceType: Literal['BiomedicalConceptProperty']
