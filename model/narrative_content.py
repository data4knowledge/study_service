from .base_node import *
from typing import List, Union, Literal
from d4kms_generic import application_logger
from .template.macros import Macros
from .template.section_number import SectionNumber

class NarrativeContent(NodeName):
  sectionNumber: str
  sectionTitle: str
  text: Union[str, None] = None
  children: List[NodeId] = []
  instanceType: Literal['NarrativeContent']

  def level(self):
    return len(self.sectionNumber.split('.'))
  
  def to_html(self, study_version):
    try:
      result = ""
      macros = Macros(study_version)
      section_number = SectionNumber(self.sectionNumber)
      if not section_number.title_sheet:
        level = section_number.level + 4
        heading_id = f"section-{self.sectionNumber}"
        result += f'<h{level} id="{heading_id}">{self.sectionNumber}&nbsp{self.sectionTitle}</h{level}>'
      result += macros.resolve(self.text, macros.AS_VALUE)
    except Exception as e:
      application_logger.exception(f"Exception raised while creating HTML from content", e)
      result = f"Exception raised while creating HTML content"

