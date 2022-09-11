from typing import List, Union
from pydantic import BaseModel
from .code import Code
from .encounter import Encounter

from uuid import UUID

class StudyEpoch(BaseModel):
  uuid: Union[UUID, None] = None
  studyEpochName: str
  studyEpochDesc: str
  previousStudyEpochId: Union[UUID, None] = None
  nextStudyEpochId: Union[UUID, None] = None
  studyEpochType: Union[Code, UUID]
  encounters: Union[List[Encounter], List[UUID]]
