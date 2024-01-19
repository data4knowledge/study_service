from typing import List
from .base_node import *
from .code import Code

class AliasCode(NodeId):
  standardCode: Code
  standardCodeAliases: List[Code] = []
