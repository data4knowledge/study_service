import os
from model.utility.utility import read_yaml_file, section_ordering
from .section_definition import SectionDefinition
from .section_number import SectionNumber
from d4kms_generic import application_logger

class TemplateDefinition():

  class MissingSection(Exception):
    pass

  def __init__(self, definition, dir, study_version):
    self._definition = definition
    self._dir = dir
    self._study_version = study_version
    if self._definition['file']:
      self._sections = read_yaml_file(os.path.join(self._dir, self._definition['file']))
    else:
      self._sections = {}
    self._section_cache = {}
    self._section_map = {}
    for k,v in self._sections.items():
      self._section_map[v['section_number']] = k

  def section_definition(self, uuid: str) -> SectionDefinition:
    if uuid in self._sections:
      if uuid in self._section_cache:
        return self._section_cache[uuid]
      else:
        section = SectionDefinition(uuid, self._sections[uuid], self._dir, self._study_version)
        self._section_cache[uuid] = section
        return section
    else:
      message = f"Missing section '{uuid}'"
      application_logger.error(message)
      raise self.MissingSection(message)

  def section_definition_by_section_number(self, section_number):
    return self.section_definition(self._section_map[section_number])

  def add_section_definition(self, uuid, data):
    #print(f"ADD: {uuid}={data}")
    self._sections[uuid] = data
    self._section_map[data['section_number']] = uuid

  def section_list(self):
    order = self._section_order()
    return [dict(self._sections[x], **{'uuid': x}) for x in order]

  def top_level_section_list(self):
    order = self._section_order()
    return [dict(self._sections[x], **{'uuid': x}) for x in order if self._level(self._definition[x]['sectionNumber']) == 1]
 
  def section_hierarchy(self):
    order = self._section_order()
    #print(f"ORDER: {order}")
    parent = [{'item': {'uuid': None, 'name':	'ROOT', 'section_number': '-1', 'section_title':	'Root', 'text': ''}, 'children': []}]
    current_level = 1
    previous_item = None
    for uuid in order:
      section = self._sections[uuid]
      section['uuid'] = uuid
      #print(f"SECTION: {section}")
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
    ordered = sorted(self._sections.items(), key=section_ordering)
    return [x[0] for x in ordered]

  # def _section_ordering(self, s):
  #   try:
  #     return [int(_) for _ in s[1]['section_number'].split(".")]
  #   except Exception as e:
  #     application_logger.exception("Exception during section ordering", e)

  def _read_sections(self):
    return read_yaml_file(os.path.join(self._dir, self._definition['file']))