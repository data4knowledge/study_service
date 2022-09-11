from typing import List, Union
from pydantic import BaseModel
from .transition_rule import TransitionRule
from .encounter import Encounter
from .activity import Activity

from uuid import UUID

class StudyElement(BaseModel):
  uuid: Union[UUID, None] = None
  studyElementName: str
  studyElementDesc: str
  transitionStartRule: Union[TransitionRule, UUID, None] = None
  transitionEndRule: Union[TransitionRule, UUID, None] = None
