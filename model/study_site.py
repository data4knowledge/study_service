from typing import Literal, Union
from .base_node import *
from .geographic_scope import SubjectEnrollment

class StudySite(NodeNameLabelDesc):
  currentEnrollment: Union[SubjectEnrollment, None] = None
  instanceType: Literal['StudySite']
