from typing import List, Union
from model.base_node import BaseNode
from datetime import datetime
from model.registration_authority import RegistrationAuthority
from uuid import UUID

class RegistrationStatus(BaseNode):
  uuid: Union[UUID, None]
  registration_status: str = ""
  effective_date: str = ""
  until_date: str = ""
  managed_by: Union[RegistrationAuthority, UUID, None] = None
