from typing import List, Union, Literal
from .base_node import *

class NarrativeContent(NodeName):
  sectionNumber: str
  sectionTitle: str
  text: Union[str, None] = None
  children: List[NodeId] = []
  instanceType: Literal['NarrativeContent']

  def level(self):
    return len(self.sectionNumber.split('.'))