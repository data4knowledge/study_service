from typing import List, Union
from pydantic import BaseModel
from .code import Code
from uuid import UUID

class IntercurrentEvent(BaseModel):
  uuid: Union[UUID, None] = None
  intercurrentEventName: str
  intercurrentEventDesc: str
  intercurrentEventStrategy: str
