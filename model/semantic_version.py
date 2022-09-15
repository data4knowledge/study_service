class SemanticVersion():

  def __init__(self, major=0, minor=0, patch=0):
    self.major = major
    self.minor = minor
    self.patch = patch

  def draft(self):
    self.major = 0
    self.minor = 1
    self.patch = 0

  def __str__(self):
    return "%s.%s.%s" % (self.major, self.minor, self.patch)