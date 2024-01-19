from typing import List
from .base_node import *
from datetime import date
from .code import Code
from .geographic_scope import GeographicScope

class GovernanceDate(NodeNameLabelDesc):
  type: Code
  dateValue: date
  geographicScopes: List[GeographicScope]