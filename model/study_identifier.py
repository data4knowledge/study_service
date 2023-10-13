from pydantic import BaseModel
from model.node import Node
from typing import Union
from uuid import UUID

from .organisation import Organisation

class StudyIdentifierIn(BaseModel):
  identifier: str
  name: str=""
  scheme: str=""
  scheme_identifier: str=""

class StudyIdentifier(Node):
  uuid: Union[UUID, None]
  studyIdentifier: str
  studyIdentifierScope: Union[UUID, Organisation, None] = None

