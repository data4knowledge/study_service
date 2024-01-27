from typing import List, Union, Literal
from .base_node import *
from .alias_code import AliasCode

class BiomedicalConceptCategory(NodeNameLabelDesc):
  children: List[NodeId] = []
  members: List[NodeId] = []
  code: Union[AliasCode, None] = None
  instanceType: Literal['BiomedicalConceptCategory']
