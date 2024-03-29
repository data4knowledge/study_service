from typing import List
from model.base_node import BaseNode
from model.variable import Variable

class Domain(BaseNode):
  uri: str
  name: str
  label: str
  structure: str
  ordinal: int
  items: List[Variable] = []
  bc_references: List[str] = []