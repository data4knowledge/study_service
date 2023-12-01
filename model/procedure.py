from typing import Union
from .node import *
from .code import Code
#from .study_intervention import StudyIntervention

class Procedure(NodeNameLabelDesc):
  procedureType: str
  code: Code
  isConditional: bool
  isConditionalReason: Union[str, None] = None
  #studyInterventionId: Union[str, None] = None