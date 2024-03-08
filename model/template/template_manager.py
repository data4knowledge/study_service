import os
from d4kms_generic.logger import application_logger
from .template_definition import TemplateDefinition
from model.utility.utility import read_yaml_file

class TemplateManager():

  class MissingTemplate(Exception):
    pass

  DIR = 'data'
  FILENAME = 'templates.yaml'

  def __init__(self):
    self._definitions = read_yaml_file(os.path.join(self.DIR, self.FILENAME))
    self._template_cache = {}

  def templates(self):
    result = [dict(v, **{'id': k}) for k,v in self._definitions.items()]
    return result
  
  def template(self, uuid: str, study_version) -> TemplateDefinition:
    if uuid in self._definitions:
      if uuid in self._template_cache:
        return self._template_cache[uuid]
      else:
        template = TemplateDefinition(self._definitions[uuid], self.DIR, study_version)
        self._template_cache[uuid] = template
        return template
    else:
      message = f"Missing template '{uuid}'"
      application_logger.error(message)
      raise self.MissingTemplate(message)

template_manager = TemplateManager()