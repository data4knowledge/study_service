from typing import List, Literal, Union
from .base_node import *
from .code import Code
from .address import Address
from .study_site import StudySite

class Organization(NodeNameLabel):
  organizationType: Code
  identifierScheme: str
  identifier: str
  legalAddress: Union[Address, None] = None
  instanceType: Literal['Organization']

class ResearchOrganization(Organization):
  manages: List[StudySite]
  instanceType: Literal['ResearchOrganization']
