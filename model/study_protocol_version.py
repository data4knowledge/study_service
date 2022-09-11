from datetime import date
from typing import List, Union
from pydantic import BaseModel
from .code import Code
from uuid import UUID

class StudyProtocolVersion(BaseModel):
  uuid: Union[UUID, None]
  briefTitle: str
  officialTitle: str
  publicTitle: str
  scientificTitle: str
  protocolVersion: str
  protocolAmendment: Union[str, None] = None
  protocolEffectiveDate: date
  protocolStatus: Union[UUID, Code]


