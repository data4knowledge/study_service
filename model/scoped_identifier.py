from pydantic import BaseModel
from model.semantic_version import SemanticVersion
from model.namespace import Namespace

class ScopedIdentifier(BaseModel):
  identifier: str
  version: int
  semantic_version: SemanticVersion
  scoped_by: Namespace
