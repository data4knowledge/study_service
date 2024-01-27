from typing import Union, Literal
from .base_node import *
from .syntax_template_dictionary import SyntaxTemplateDictionary

class SyntaxTemplate(NodeNameLabelDesc):
  text: str
  dictionary: Union[SyntaxTemplateDictionary, None] = None
  instanceType: Literal['SyntaxTemplate']
