from .base_node import *
from typing import Union
from uuid import UUID

class Code(NodeId):
  code: str
  codeSystem: str
  codeSystemVersion: str
  decode: str
  
