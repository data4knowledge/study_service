from pydantic import BaseModel
from model.semantic_version import SemanticVersion
from model.namespace import Namespace
from uuid import UUID

class ScopedIdentifier(BaseModel):
  uuid: UUID
  identifier: str
  version: int
  semantic_version: SemanticVersion
  scoped_by: Namespace
