from typing import Literal
from pydantic import BaseModel
from uuid import UUID

class TransitionRule(BaseModel):
  text: str
  instanceType: Literal['TransitionRule']