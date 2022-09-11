from typing import Union
from uuid import UUID
from pydantic import BaseModel
from .code import Code

class Organisation(BaseModel):
  uuid: Union[UUID, None]
  organisationIdentifierScheme: str
  organisationIdentifier: str
  organisationName: str
  organisationType: Union[UUID, Code]

  @classmethod
  def global_reuse(cls):
    return True

