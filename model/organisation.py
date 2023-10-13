from typing import Union
from uuid import UUID
from model.node import Node
from model.code import Code

class Organisation(Node):
  uuid: Union[UUID, None]
  organisationIdentifierScheme: str
  organisationIdentifier: str
  organisationName: str
  organisationType: Union[UUID, Code, None] = None

  @classmethod
  def global_reuse(cls):
    return True

