from typing import Literal, Union
from .base_node import *
from .code import Code

class Range(NodeId):
  minValue: float
  maxValue: float
  unit: Union[Code, None] = None
  isApproximate: bool
  instanceType: Literal['Range']
