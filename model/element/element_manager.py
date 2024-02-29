import os
from d4kms_generic.logger import application_logger
from model.utility.utility import read_yaml_file
from .element import Element

class ElementManager():

  DIR = 'data'
  FILENAME = 'elements.yaml'

  class MissingElement(Exception):
    pass

  def __init__(self, study_version):
    self._study_version = study_version
    self._definitions = read_yaml_file(os.path.join(self.DIR, self.FILENAME))

  def element(self, element_name: str) -> Element:
    if element_name in self._definitions:
      return Element(self._study_version, self._definitions[element_name])
    else:
      message = f"Missing element '{element_name}'"
      application_logger.error(message)
      raise self.MissingElement(message)
