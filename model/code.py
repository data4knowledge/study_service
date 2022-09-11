from typing import Union
from uuid import UUID
from pydantic import BaseModel

class Code(BaseModel):
  uuid: Union[UUID, None]
  code: str
  codeSystem: str
  codeSystemVersion: str
  decode: str
  
  # @classmethod
  # def global_reuse(cls):
  #   return True

