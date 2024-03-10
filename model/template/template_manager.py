import os
from d4kms_generic.logger import application_logger
from model.template.template_definition import TemplateDefinition
from model.template.section_number import SectionNumber
from model.utility.utility import read_yaml_file
from uuid import uuid4

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

  def template_from_sections(self, ncs, study_version) -> TemplateDefinition:
    uuid = str(uuid4())
    self._definitions[uuid] = {'name': uuid, 'file': None}
    template = TemplateDefinition(self._definitions[uuid], self.DIR, study_version)
    self._template_cache[uuid] = template
    for section, nc in ncs.items():
      print(f"ITEM: {nc.sectionNumber}")
      section_number = SectionNumber(nc.sectionNumber)
      data = {
        'file': 'text_section.html',
        'header_only': False,
        'level': section_number.level,
        'section_number': nc.sectionNumber,
        'section_title': nc.sectionTitle,
        'display_heading': True
      }
      template.add_section_definition(str(uuid4()), data)
    return uuid, template

template_manager = TemplateManager()