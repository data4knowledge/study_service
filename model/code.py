from typing import Literal
from .base_node import *

class Code(NodeId):
  code: str
  codeSystem: str
  codeSystemVersion: str
  decode: str
  instanceType: Literal['Code']
