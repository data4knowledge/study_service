from typing import Literal
from .base_node import *

class IntercurrentEvent(NodeNameLabelDesc):
  strategy: str
  instanceType: Literal['IntercurrentEvent']
