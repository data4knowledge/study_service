from typing import List, Union
from pydantic import BaseModel
from .code import Code
from uuid import UUID

class InvestigationalIntervention(BaseModel):
  uuid: Union[UUID, None] = None
  codes: Union[List[Code], List[UUID], None]  
  interventionDesc: str
