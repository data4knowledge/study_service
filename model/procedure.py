from typing import Union
from pydantic import BaseModel
from .code import Code
from uuid import UUID

class PreviousProcedure(BaseModel):
  procedureName: str
  procedureCode: Union[Code, UUID]

class Procedure(BaseModel):
  uuid: Union[UUID, None] = None
  procedureType: str
  procedureCode: Union[Code, UUID]
  
  @classmethod
  def scope_reuse(cls):
    return True