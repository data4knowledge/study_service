from typing import Union
from .base_node import *
from .code import Code

class StudyAmendmentReason(NodeId):
  code: Code
  otherReason: Union[str, None] = None
