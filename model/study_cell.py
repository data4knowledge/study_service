from typing import List, Union
from pydantic import BaseModel
from .study_arm import StudyArm
from .study_epoch import StudyEpoch
from .study_element import StudyElement
from uuid import UUID

class StudyCell(BaseModel):
  uuid: Union[UUID, None] = None
  studyArm: Union[StudyArm, UUID, None]
  studyEpoch: Union[StudyEpoch, UUID, None]
  studyElements: Union[List[StudyElement], List[UUID], None] = []
