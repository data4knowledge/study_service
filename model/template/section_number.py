class SectionNumber():

  def __init__(self, section_number):
    section_number = section_number[:-1] if section_number.endswith('.') else section_number
    if section_number:
      self._parts = section_number.split('.')
      self.level = len(self._parts)
      self.number = '.'.join(self._parts)
      #self.title_sheet = False
    else:
      self._parts = []
      self.level = 1
      self.number = ''
      #self.title_sheet = True

  def part(self, level):
    return self._parts[level - 1] if level <= self.level else None
