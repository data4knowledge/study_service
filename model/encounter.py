from typing import List, Union
from pydantic import BaseModel
from .code import Code
from .transition_rule import TransitionRule
from .activity import Activity
from uuid import UUID

class Encounter(BaseModel):
  uuid: Union[UUID, None] = None
  encounterName: str
  encounterDesc: str
  previousEncounterId: Union[UUID, None] = None
  nextEncounterId: Union[UUID, None] = None
  encounterType: Union[Code, UUID, None] = None
  encounterEnvironmentalSetting: Union[Code, UUID, None] = None
  encounterContactMode: Union[Code, UUID, None] = None
  transitionStartRule: Union[TransitionRule, UUID, None] = None
  transitionEndRule: Union[TransitionRule, UUID, None] = None
