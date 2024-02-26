import os
from model.template.template_base import TemplateBase
from model.template.section import Section
from model.section_number import SectionNumber
from d4kms_generic.logger import application_logger

class Template(TemplateBase):

  class MissingSection(Exception):
    pass

  def __init__(self, definition, dir):
    self._definition = definition
    self._dir = dir
    self._sections = self._read_sections()

  def section(self, uuid):
    if uuid in self._definitions:
      return Section(self._definitions[uuid])
    else:
      message = f"Missing section '{uuid}'"
      application_logger.error(message)
      raise self.MissingSection(message)

  def section_list(self):
    order = self._section_order()
    return [{'key': x, 'sectionNumber': self._definition[x]['sectionNumber'], 'sectionTitle': self._definition[x]['sectionTitle']} for x in order]

  def top_level_section_list(self):
    order = self._section_order()
    return [{'key': x, 'sectionNumber': self._definition[x]['sectionNumber'], 'sectionTitle': self._definition[x]['sectionTitle']} for x in order if self._level(self._definition[x]['sectionNumber']) == 1]
 
  def section_hierarchy(self):
    order = self._section_order()
    print(f"ORDER: {order}")
    parent = [{'item': {'name':	'ROOT', 'section_number': '-1', 'section_title':	'Root', 'text': ''}, 'children': []}]
    current_level = 1
    previous_item = None
    for uuid in order:
      section = self._sections[uuid]
      print(f"SECTION: {section}")
      section_number = SectionNumber(section['section_number'])
      item = {'item': section, 'children': []}
      if section_number.level == current_level:
        parent[-1]['children'].append(item)
      elif section_number.level > current_level:
        parent.append(previous_item)
        parent[-1]['children'].append(item)
        current_level = section_number.level
      elif section_number.level < current_level:
        parent.pop()
        parent[-1]['children'].append(item)
        current_level = section_number.level
      current_level = section_number.level
      previous_item = item
    return parent[0]

  def _section_order(self):
    ordered = sorted(self._sections.items(), key=self._section_ordering)
    return [x[0] for x in ordered]

  def _section_ordering(self, s):
    try:
      return [int(_) for _ in s[1]['section_number'].split(".")]
    except Exception as e:
      application_logger.exception("Exception during section ordering", e)

  def _read_sections(self):
    return self.read_yaml_file(os.path.join(self._dir, self._definition['file']))