from typing import Literal
from .base_node import NodeId
from .code import Code

class StudyTitle(NodeId):
  text: str
  type: Code
  instanceType: Literal['StudyTitle']
