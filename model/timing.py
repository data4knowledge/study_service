from typing import Union, Literal
from .base_node import *
from .code import Code

class Timing(NodeNameLabelDesc):
  type: Code
  value: str
  relativeToFrom: Code
  relativeFromScheduledInstanceId: Union[str, None] = None
  relativeToScheduledInstanceId: Union[str, None] = None
  windowLower: Union[str, None] = None
  windowUpper: Union[str, None] = None
  window: Union[str, None] = None
  instanceType: Literal['Timing']