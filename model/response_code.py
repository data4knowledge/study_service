from typing import Literal
from .base_node import *
from .code import Code

class ResponseCode(NodeId):
  isEnabled: bool
  code: Code
  instanceType: Literal['ResponseCode']
