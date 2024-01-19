from typing import Union, List
from .base_node import *
from .study_amendment_reason import StudyAmendmentReason
from .subject_enrollment import SubjectEnrollment

class StudyAmendment(NodeId):
  number: str
  summary: str
  substantialImpact: bool
  primaryReason: StudyAmendmentReason
  secondaryReasons: List[StudyAmendmentReason] = []
  enrollments: List[SubjectEnrollment]
  previousId: Union[str, None] = None
