from pydantic import BaseModel
from model.node import Node
from typing import Union
from uuid import UUID

class Namespace(Node):
  uuid: Union[UUID, None]
  namespace_uri: str
