from typing import Dict, Literal
from .base_node import *

class SyntaxTemplateDictionary(NodeNameLabelDesc):
  parameterMap: Dict
  instanceType: Literal['SyntaxTemplateDictionary']
