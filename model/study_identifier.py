from .node import *
from .organization import Organization

class StudyIdentifier(NodeId):
  studyIdentifier: str
  studyIdentifierScope: Organization
