from model.template.template_base import TemplateBase
from model.template.section import Section
from d4kms_generic.logger import application_logger

class Template(TemplateBase):

  class MissingSection(Exception):
    pass

  def __init__(self, definition):
    self._definition = definition

  def section(self, name):
    if name in self._definitions:
      return Section(self._definitions[name])
    else:
      message = f"Missing section '{name}'"
      application_logger.error(message)
      raise self.MissingSection(message)

  def section_list(self):
    order = self._section_order()
    return [{'key': x, 'sectionNumber': self._definition[x]['sectionNumber'], 'sectionTitle': self._definition[x]['sectionTitle']} for x in order]

  def top_level_section_list(self):
    order = self._section_order()
    return [{'key': x, 'sectionNumber': self._definition[x]['sectionNumber'], 'sectionTitle': self._definition[x]['sectionTitle']} for x in order if self._level(self._definition[x]['sectionNumber']) == 1]
 
  def _section_order(self):
    return sorted(list(self._definition.keys()), key=self._section_ordering)

  def _section_ordering(self, s):
    try:
      return [int(_) for _ in s.split("-")]
    except Exception as e:
      application_logger.exception("Exception during section ordering", e)
