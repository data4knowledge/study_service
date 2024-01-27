from typing import Literal, Union
from .base_node import *
from .code import Code

class Quantity(NodeId):
  value: float
  unit: Union[Code, None] = None
  instanceType: Literal['Quantity']
