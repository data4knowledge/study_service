from typing import List, Union
from pydantic import BaseModel
from .code import Code
from .workflow_item import WorkflowItem
from uuid import UUID, uuid4

class WorkflowIn(BaseModel):
  name: str # EXTENSION
  description: str

class Workflow(BaseModel):
  uuid: Union[UUID, None] = None
  workflowName: str # EXTENSION
  workflowDesc: str
  workflowItems: Union[List[WorkflowItem], List[UUID], None] = []