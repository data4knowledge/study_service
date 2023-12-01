from typing import List
from .alias_code import AliasCode
from model.node import *
from .response_code import ResponseCode

class BiomedicalConceptProperty(NodeNameLabel):
  isRequired: bool
  isEnabled: bool
  datatype: str
  responseCodes: List[ResponseCode] = []
  code: AliasCode
