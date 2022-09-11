from typing import List, Union
from pydantic import BaseModel
from .code import Code
from uuid import UUID

class Indication(BaseModel):
  uuid: Union[UUID, None] = None
  codes: Union[List[Code], List[UUID], None]
  indicationDesc: str
