from typing import List, Union
from .base_node import *
from .alias_code import AliasCode

class BiomedicalConceptCategory(NodeNameLabelDesc):
  #childrenIds: List[str] = []
  #memberIds: List[str] = []
  code: Union[AliasCode, None] = None
