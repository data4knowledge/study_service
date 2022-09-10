from pydantic import BaseModel
from model.semantic_version import SemanticVersion

class ScopedIdentifier(BaseModel):
  identifier: str
  version: int
  semantic_version: SemanticVersion
