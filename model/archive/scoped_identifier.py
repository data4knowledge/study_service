from typing import List, Union
from model.base_node import BaseNode
from model.namespace import Namespace
from uuid import UUID

class ScopedIdentifier(BaseNode):
  uuid: UUID
  identifier: str
  version: int
  semantic_version: str
  scoped_by: Union[Namespace, UUID, None] = None
