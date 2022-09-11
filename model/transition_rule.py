from typing import Union
from pydantic import BaseModel
from uuid import UUID

class TransitionRule(BaseModel):
  uuid: Union[UUID, None] = None
  transitionRuleDesc: str