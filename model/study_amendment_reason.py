from typing import Union
from .node import *
from .code import Code

class StudyAmendmentReason(NodeId):
  code: Code
  otherReason: Union[str, None] = None
