from model.template.template_base import TemplateBase
from d4kms_generic.logger import application_logger

class Section(TemplateBase):

  def __init__(self, definition):
    self._definition = definition

  def get(self):
    return self._definition