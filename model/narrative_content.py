from typing import List, Union
from .base_node import *

class NarrativeContent(NodeName):
  sectionNumber: str
  sectionTitle: str
  text: Union[str, None] = None
  contentChildIds: List[str] = []

  def level(self):
    return len(self.sectionNumber.split('.'))