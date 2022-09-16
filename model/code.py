from model.node import Node
from typing import Union
from uuid import UUID

class Code(Node):
  uuid: Union[UUID, None]
  code: str
  codeSystem: str
  codeSystemVersion: str
  decode: str
  
  # @classmethod
  # def global_reuse(cls):
  #   return True

