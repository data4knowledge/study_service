from typing import List, Union
from pydantic import BaseModel
from .code import Code
from uuid import UUID

class Endpoint(BaseModel):
  uuid: Union[UUID, None] = None
  endpointDesc: str
  endpointPurposeDesc: str
  endpointLevel: Union[Code, UUID, None]