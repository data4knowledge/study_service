from .node import *
from .code import Code

class ResponseCode(NodeId):
  isEnabled: bool
  code: Code
