from typing import Union, List, Literal
from .base_node import *
from .study_amendment_reason import StudyAmendmentReason
from .geographic_scope import SubjectEnrollment

class StudyAmendment(NodeId):
  number: str
  summary: str
  substantialImpact: bool
  primaryReason: StudyAmendmentReason
  secondaryReasons: List[StudyAmendmentReason] = []
  enrollments: List[SubjectEnrollment]
  previousId: Union[str, None] = None
  instanceType: Literal['StudyAmendment']
