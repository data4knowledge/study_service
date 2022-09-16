from typing import List, Union
from model.node import Node
from model.namespace import Namespace
from uuid import UUID

class ScopedIdentifier(Node):
  uuid: UUID
  identifier: str
  version: int
  semantic_version: str
  scoped_by: Union[Namespace, UUID, None]
