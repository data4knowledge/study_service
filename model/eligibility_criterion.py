from typing import Literal, Union
from .base_node import *
from .syntax_template import SyntaxTemplate
from .code import Code

class EligibilityCriterion(SyntaxTemplate):
  category: Code
  identifier: str
  next: Union[NodeId, None] = None
  previous: Union[NodeId, None] = None
  context: Union[NodeId, None] = None
  instanceType: Literal['EligibilityCriterion']
