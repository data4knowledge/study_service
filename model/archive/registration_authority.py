from model.base_node import BaseNode
from typing import List, Union
from uuid import UUID

class RegistrationAuthority(BaseNode):
  uuid: Union[UUID, None]
  name: str = ""
  ra_uuid: str = ""
  