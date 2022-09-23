from typing import Union
from pydantic import BaseModel
from uuid import UUID

class StudyDataIn(BaseModel):
  name: str
  description: str
  link: str

class StudyData(BaseModel):
  uuid: Union[UUID, None] = None
  #uri: str
  studyDataName: str
  studyDataDesc: str
  crfLink: str
