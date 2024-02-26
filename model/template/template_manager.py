import os
from d4kms_generic.logger import application_logger
from model.template.template_base import TemplateBase
from model.template.template import Template

class TemplatetManager(TemplateBase):

  class MissingTemplate(Exception):
    pass

  DIR = 'data'
  FILENAME = 'templates.yaml'

  def __init__(self):
    self._definitions = self.read_yaml_file(os.path.join(self.DIR, self.FILENAME))

  def templates(self):
    print(f"DEFS: {self._definitions}")
    #result = [v for k,v in self._definitions.items()]
    result = [dict(v, **{'id': k}) for k,v in self._definitions.items()]
    print(f"TEMPLATES: {result}")
    return result
  
  def template(self, uuid: str) -> Template:
    if uuid in self._definitions:
      return Template(self._definitions[uuid], self.DIR)
    else:
      message = f"Missing template '{uuid}'"
      application_logger.error(message)
      raise self.MissingTemplate(message)
