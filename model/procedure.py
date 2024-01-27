from typing import Literal, Union
from .base_node import *
from .code import Code

class Procedure(NodeNameLabelDesc):
  procedureType: str
  code: Code
  studyInterventionId: Union[str, None] = None
  instanceType: Literal['Procedure']