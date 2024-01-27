from typing import Union, List, Literal
from .base_node import *
from .transition_rule import TransitionRule

class StudyElement(NodeNameLabelDesc):
  transitionStartRule: Union[TransitionRule, None] = None
  transitionEndRule: Union[TransitionRule, None] = None
  studyInterventionIds: List[str] = []
  instanceType: Literal['StudyElement']