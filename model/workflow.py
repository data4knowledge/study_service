from typing import List, Union
from pydantic import BaseModel
from .code import Code
from .workflow_item import WorkflowItem
from uuid import UUID

class Workflow(BaseModel):
  uuid: Union[UUID, None] = None
  workflowDesc: str
  workflowItems: Union[List[WorkflowItem], List[UUID], None]