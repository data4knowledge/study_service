from typing import List, Union, Literal
from pydantic import BaseModel
from .study_arm import StudyArm
from .study_epoch import StudyEpoch
from .study_element import StudyElement
from uuid import UUID

class StudyCell(BaseModel):
  studyArm: Union[StudyArm, None]
  studyEpoch: Union[StudyEpoch, None]
  studyElements: Union[List[StudyElement], None] = []
  instanceType: Literal['StudyCell']
