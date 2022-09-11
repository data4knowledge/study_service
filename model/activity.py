from typing import List, Union
from pydantic import BaseModel
from .procedure import Procedure
from .study_data import StudyData

from uuid import UUID

class Activity(BaseModel):
  uuid: Union[UUID, None] = None
  activityName: str
  activityDesc: str
  previousActivityId: Union[UUID, None] = None
  nextActivityId: Union[UUID, None] = None
  definedProcedures: Union[List[Procedure], List[UUID]] = []
  studyDataCollection: Union[List[StudyData], List[UUID]] = []
