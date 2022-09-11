from pydantic import BaseModel
from typing import Union
from uuid import UUID

from .organisation import Organisation

class StudyIdentifier(BaseModel):
  uuid: Union[UUID, None]
  studyIdentifier: str
  studyIdentifierScope: Union[UUID, Organisation]

