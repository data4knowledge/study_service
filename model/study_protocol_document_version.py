import yaml
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
  protocolStatus: Code = None # 
  dateValues: List[GovernanceDate] = []
  contents: List[NarrativeContent] = []
  childrenIds: List[str] = []

  def section_list(self):
    return {'root': self._read_section_list()}

  def _read_section_list(self):
    return self._read_as_yaml_file("data/m11_sections.yaml")
  
  def _read_as_yaml_file(self, filepath):
    with open(filepath, "r") as f:
      data = yaml.load(f, Loader=yaml.FullLoader)
    return data