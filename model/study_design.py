from typing import List, Union
from pydantic import BaseModel
from .code import Code
from .study_cell import StudyCell
from .indication import Indication
from .investigational_intervention import InvestigationalIntervention
from .study_design_population import StudyDesignPopulation
from .objective import Objective
from .workflow import Workflow
from .workflow_item import WorkflowItem
from .estimand import Estimand
from uuid import UUID
#import pandas as pd

class StudyDesign(BaseModel):
  
  def __init__(self):
    self.uuid = ""
    self.uri = ""
    self.trialType = None
    self.interventionModel = None
    self.studyCells = []
    self.studyIndications  = []
    self.studyInvestigationalInterventions = []
    self.studyPopulations = []
    self.studyObjectives = []
    self.studyWorkflows = []
    self.studyEstimands = []
    self.studyActivities = []
    self.studyEncounters = []

