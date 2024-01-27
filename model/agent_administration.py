from .base_node import *
from .quantity import Quantity
from .administration_duration import AdministrationDuration
from .code import Code
from typing import Literal

class AgentAdministration(NodeNameLabelDesc):
  duration:	AdministrationDuration
  dose:	Quantity
  route:	Code
  frequency:	Code
  instanceType: Literal['AgentAdministration']
