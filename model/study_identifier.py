from model.node import Node
from typing import Union
from uuid import UUID

from .organisation import Organisation

class StudyIdentifier(Node):
  uuid: Union[UUID, None]
  studyIdentifier: str
  studyIdentifierScope: Union[UUID, Organisation, None]

