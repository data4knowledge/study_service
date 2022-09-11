from typing import Union
from pydantic import BaseModel
from uuid import UUID

class StudyData(BaseModel):
  uuid: Union[UUID, None] = None
  studyDataName: str
  studyDataDesc: str
  crfLink: str
