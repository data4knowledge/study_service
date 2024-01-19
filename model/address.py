from typing import Union
from .base_node import *
from .code import Code

class Address(NodeId):
  text: str = ""
  line: str = ""
  city: str = ""
  district: str = ""
  state: str = ""
  postalCode: str = ""
  country: Union[Code, None] = None
