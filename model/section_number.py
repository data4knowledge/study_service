class SectionNumber():

  def __init__(self, section_number):
    self._parts = section_number.split('.')
    self.level = len(self._parts)

  def part(self, level):
    return self._parts[level -1] if level <= self.level else None