from .base_node import *
from .quantity import Quantity
from typing import Literal

class AdministrationDuration(NodeId):
  quantity:	Quantity
  description: str
  durationWillVary: bool
  reasonDurationWillVary:	str
  instanceType: Literal['AdministrationDuration']
