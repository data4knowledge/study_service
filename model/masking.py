from typing import Literal
from .base_node import *
from .code import Code

class Masking(NodeDesc):
  role: Code
  instanceType: Literal['Masking']
