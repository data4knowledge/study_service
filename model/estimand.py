from typing import List, Literal
from .base_node import *
from .analysis_population import AnalysisPopulation
from .investigational_intervention import InvestigationalIntervention
from .endpoint import Endpoint
from .intercurrent_event import IntercurrentEvent
from uuid import UUID

class Estimand(NodeId):
  summaryMeasure: str
  analysisPopulation: AnalysisPopulation
  interventionId: str
  variableOfInterestId: str
  intercurrentEvents: List[IntercurrentEvent]
  instanceType: Literal['Estimand']
