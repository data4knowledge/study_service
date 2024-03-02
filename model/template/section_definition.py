import os
from model.utility.utility import read_text_file
from pydantic import BaseModel
from .macros import Macros

class SectionDefinition(BaseModel):
  header_only: bool
  level: int
  section_number: str
  section_title: str
  display_heading: bool
  form: str = ""

  def __init__(self, definition, dir, study_version):
    super().__init__(**definition)
    self._form = read_text_file(os.path.join(dir, definition['file']))
    self._macros = Macros(study_version)

  def resolve(self):
    return self._macros.resolve(self._form, Macros.AS_REFERENCE)
