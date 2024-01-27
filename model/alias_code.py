from typing import List, Literal
from .base_node import *
from .code import Code

class AliasCode(NodeId):
  standardCode: Code
  standardCodeAliases: List[Code] = []
  instanceType: Literal['AliasCode']
