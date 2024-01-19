from pydantic import BaseModel
from typing import Union
from uuid import UUID
from model.base_node import BaseNode

class Namespace(BaseNode):
  uuid: Union[UUID, None]
  namespace_uri: str
