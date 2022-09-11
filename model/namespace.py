from pydantic import BaseModel
from typing import Union
from uuid import UUID

class Namespace(BaseModel):
  uuid: Union[UUID, None]
  namespace_uri = str
