from .base_node import *
from typing import List, Union, Literal
from d4kms_generic import application_logger
from .template.macros import Macros
from .template.section_number import SectionNumber
from model.template.template_manager import TemplateManager

class NarrativeContent(NodeName):
  sectionNumber: str
  sectionTitle: str
  text: Union[str, None] = None
  children: List[NodeId] = []
  instanceType: Literal['NarrativeContent']

  def level(self):
    return len(self.sectionNumber.split('.'))
  
  def to_html(self, study_version, section_definition):
    try:
      #print(f"SD: {section_definition}")
      result = ""
      macros = Macros(study_version)
      if section_definition.display_heading:
        level = section_definition.level + 4
        heading_id = f"section-{self.sectionNumber}"
        result += f'<h{level} id="{heading_id}">{self.sectionNumber}&nbsp{self.sectionTitle}</h{level}>'
      result += macros.resolve(self.text, macros.AS_VALUE)
      return result
    except Exception as e:
      application_logger.exception(f"Exception raised while creating HTML from content", e)
      return f"Exception raised while creating HTML content"

