from pydantic import BaseModel
from model.namespace import Namespace
from uuid import UUID

class ScopedIdentifier(BaseModel):
  uuid: UUID
  identifier: str
  version: int
  semantic_version: str
  scoped_by: Namespace
