from typing import Union
from pydantic import BaseModel
from .activity import Activity
from .encounter import Encounter
from uuid import UUID

class WorkflowItemIn(BaseModel):
  description: str
  encounter_uuid: str
  activity_uuid: str

class WorkflowItem(BaseModel):
  uuid: str
  workflowItemDesc: str
  previousWorkflowItemId: Union[UUID, None] = None
  nextWorkflowItemId: Union[UUID, None] = None
  workflowItemEncounter: Union[Encounter, UUID, None] = None
  workflowItemActivity: Union[Activity, UUID, None] = None
  uniqueLabel: str = ""
  