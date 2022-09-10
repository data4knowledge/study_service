class SemanticVersion():

  def __init__(self, major, minor=0, patch=0):
    self.major = major
    self.minor = minor
    self.patch = patch

  def __str__(self):
    return "%s.%s.%s" % (self.major, self.minor, self.patch)