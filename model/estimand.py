from typing import List, Union
from pydantic import BaseModel
from .analysis_population import AnalysisPopulation
from .investigational_intervention import InvestigationalIntervention
from .endpoint import Endpoint
from .intercurrent_event import IntercurrentEvent
from uuid import UUID

class Estimand(BaseModel):
  uuid: Union[UUID, None] = None
  summaryMeasure: str
  analysisPopulation: Union[AnalysisPopulation, UUID]
  treatment: Union[InvestigationalIntervention, UUID]
  variableOfInterest: Union[Endpoint, UUID]
  intercurrentEvents: Union[List[IntercurrentEvent], List[UUID]]
