from typing import List
from pydantic import BaseModel
from typing import List, Union
from model.code import *
from model.study_identifier import *
from model.study_protocol_version import *
from model.study_design import *

class Study(BaseModel):
  uuid: Union[UUID, None]
  studyTitle: str
  studyVersion: str
  studyType: Union[Code, UUID, None]
  studyPhase: Union[Code, UUID, None]
  studyIdentifiers: Union[List[StudyIdentifier], List[UUID], None] = []
  studyProtocolVersions: Union[List[StudyProtocolVersion], List[UUID], None] = []
  studyDesigns: Union[List[StudyDesign], List[UUID], None] = []

class StudyPartial(BaseModel):
  uri: str
  uuid: str
  name: str
  identified_by: dict
  has_status: dict

class StudyList(BaseModel):
  items: List[StudyPartial]
  page: int
  size: int
  filter: str
  count: int

#   @classmethod
#   def list(cls, page, size, filter):
#     if filter == "":
#       count = bci.full_count()
#     else:
#       count = bci.filter_count(filter)
#     results = {'items': [], 'page': page, 'size': size, 'filter': filter, 'count': count }
#     results['items'] = bci.list(page, size, filter)
#     return results

# class Study(BaseModel):
#   uri: str
#   uuid: str
#   name: str
#   identified_by: dict
#   has_status: dict
#   #identifier: dict
#   items: List[dict]

#   @classmethod
#   def find(cls, uuid):
#     return bci.find(uuid)