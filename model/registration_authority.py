from model.node import Node
from typing import List, Union
from uuid import UUID

class RegistrationAuthority(Node):
  uuid: Union[UUID, None]
  name: str = ""
  ra_uuid: str = ""
  