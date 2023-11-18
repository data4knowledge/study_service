from typing import Union
from .node import *
from .code import Code
from .alias_code import AliasCode

class GeographicScope(NodeId):
  type: Code
  code: Union[AliasCode, None] = None