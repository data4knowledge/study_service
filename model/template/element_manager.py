from d4kms_generic.logger import application_logger
from model.template.template_base import TemplateBase
from model.template.element import Element

class ElementManager(TemplateBase):

  class MissingElement(Exception):
    pass

  def __init__(self, filepath, study_version):
    self._study_version = study_version
    self._filpath = filepath
    self._definitions = self.read_yaml_file(self._filpath)

  def get(self, element_name: str) -> Element:
    if element_name in self._definitions:
      return Element(self._study_version, self._definitions[element_name])
    else:
      message = f"Missing element '{element_name}'"
      application_logger.error(message)
      raise self.MissingElement(message)
