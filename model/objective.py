from typing import List, Union
from pydantic import BaseModel
from .code import Code
from .endpoint import Endpoint
from uuid import UUID

class Objective(BaseModel):
  uuid: Union[UUID, None] = None
  objectiveDesc: str
  objectiveLevel: Union[Code, UUID, None]
  objectiveEndpoints: Union[List[Endpoint], List[UUID], None]