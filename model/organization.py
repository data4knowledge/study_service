from typing import Union
from .node import *
from .code import Code
from .address import Address

class Organization(NodeNameLabel):
  type: Code
  organizationIdentifierScheme: str
  organizationIdentifier: str
  organizationLegalAddress: Union[Address, None] = None
