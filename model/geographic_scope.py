from typing import Literal, Union
from .base_node import *
from .code import Code
from .alias_code import AliasCode
from .quantity import Quantity

class GeographicScope(NodeId):
  type: Code
  code: Union[AliasCode, None] = None
  instanceType: Literal['GeographicScope']

class SubjectEnrollment(GeographicScope):
  quantity: Quantity
  instanceType: Literal['SubjectEnrollment']