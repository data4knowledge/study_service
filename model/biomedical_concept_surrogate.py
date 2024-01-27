from typing import Literal, Union
from .base_node import *

class BiomedicalConceptSurrogate(NodeNameLabelDesc):
  reference: Union[str, None] = None
  instanceType: Literal['BiomedicalConceptSurrogate']
