from typing import List, Literal
from .base_node import *
from .code import Code

class Indication(NodeNameLabelDesc):
  codes: List[Code] = []
  isRareDisease: bool
  instanceType: Literal['Indication']
