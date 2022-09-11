from typing import List, Union
from pydantic import BaseModel
from uuid import UUID

class StudyDesignPopulation(BaseModel):
  uuid: Union[UUID, None] = None
  populationDesc: str
