import os
from model.utility.utility import read_text_file
from pydantic import BaseModel

class SectionDefinition(BaseModel):
  header_only: bool
  level: int
  section_number: str
  section_title: str
  display_heading: bool
  form: str = ""

  def __init__(self, definition, dir):
    super().__init__(**definition)
    self.form = read_text_file(os.path.join(dir, definition['file']))
