from typing import List
from pydantic import BaseModel
from typing import List, Union

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