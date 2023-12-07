class SectionNumber():

  def __init__(self, section_number):
    if section_number:
      self._parts = section_number.split('.')
      self.level = len(self._parts)
      self.title_sheet = False
    else:
      self._parts = []
      self.level = 1
      self.title_sheet = True

  def part(self, level):
    return self._parts[level -1] if level <= self.level else None
  
