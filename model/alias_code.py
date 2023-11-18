from typing import List
from .node import *
from .code import Code

class AliasCode(NodeId):
  standardCode: Code
  standardCodeAliases: List[Code] = []
