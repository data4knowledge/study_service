from typing import List, Union
from model.node import Node
from datetime import datetime
from model.registration_authority import RegistrationAuthority
from uuid import UUID

class RegistrationStatus(Node):
  uuid: Union[UUID, None]
  registration_status: str = ""
  effective_date: str = ""
  until_date: str = ""
  managed_by: Union[RegistrationAuthority, UUID, None] = None
