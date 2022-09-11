from pydantic import BaseModel

class SemanticVersion(BaseModel):
  major = str
  minor = str
  patch = str

  def draft(self):
    major = 0
    minor = 1
    patch = 0

  def __str__(self):
    return "%s.%s.%s" % (self.major, self.minor, self.patch)