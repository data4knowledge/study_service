from typing import List
from .node import *
from .code import Code
from .governance_date import GovernanceDate
from .narrative_content import NarrativeContent

class StudyProtocolDocumentVersion(NodeId):
  briefTitle: str
  officialTitle: str
  publicTitle: str
  scientificTitle: str
  protocolVersion: str
  protocolStatus: Code
  dateValues: List[GovernanceDate] = []
  contents: List[NarrativeContent] = []
  childrenIds: List[str] = []
