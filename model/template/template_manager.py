from d4kms_generic.logger import application_logger
from model.template.template_base import TemplateBase
from model.template.template import Template

class TemplatetManager(TemplateBase):

  class MissingTemplate(Exception):
    pass

  def __init__(self, filepath):
    self._filepath = filepath
    self._definitions = self.read_yaml_file(self._filepath)

  def get(self, name: str) -> Template:
    if name in self._definitions:
      return Template(self._definitions[name])
    else:
      message = f"Missing template '{name}'"
      application_logger.error(message)
      raise self.MissingTemplate(message)
