import os
from model.utility.utility import read_text_file
from pydantic import BaseModel
from .macros import Macros

class SectionDefinition(BaseModel):
  uuid: str
  header_only: bool
  level: int
  section_number: str
  section_title: str
  display_heading: bool
  section_uuid: str = None
  form: str = ""

  def __init__(self, uuid, definition, dir, study_version):
    definition['uuid'] = uuid
    super().__init__(**definition)
    self.form = read_text_file(os.path.join(dir, definition['file']))
    self._macros = Macros(study_version)

  def resolve(self):
    return self._macros.resolve(self.form, Macros.AS_REFERENCE)
