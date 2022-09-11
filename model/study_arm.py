from typing import Union
from pydantic import BaseModel
from .code import Code
from uuid import UUID

class StudyArm(BaseModel):
  uuid: Union[UUID, None] = None
  studyArmName: str
  studyArmDesc: str
  studyArmType: Union[Code, UUID]
  studyArmDataOriginDesc: str
  studyArmDataOriginType: Union[Code, UUID]
