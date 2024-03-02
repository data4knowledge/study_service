import os
from d4kms_generic.logger import application_logger
from .template_definition import TemplateDefinition
from model.utility.utility import read_yaml_file

class TemplatetManager():

  class MissingTemplate(Exception):
    pass

  DIR = 'data'
  FILENAME = 'templates.yaml'

  def __init__(self, study_version):
    self._study_version = study_version
    self._definitions = read_yaml_file(os.path.join(self.DIR, self.FILENAME))

  def templates(self):
    result = [dict(v, **{'id': k}) for k,v in self._definitions.items()]
    return result
  
  def template(self, uuid: str) -> TemplateDefinition:
    if uuid in self._definitions:
      return TemplateDefinition(self._definitions[uuid], self.DIR, self._study_version)
    else:
      message = f"Missing template '{uuid}'"
      application_logger.error(message)
      raise self.MissingTemplate(message)
