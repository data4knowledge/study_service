from typing import List, Union
from pydantic import BaseModel

class AnalysisPopulation(BaseModel):
  uuid: Union[str, None] = None
  populationDesc: str
