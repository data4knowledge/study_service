from typing import Literal
from .base_node import *
from .organization import Organization

class StudyIdentifier(NodeId):
  studyIdentifier: str
  studyIdentifierScope: Organization
  instanceType: Literal['StudyIdentifier']