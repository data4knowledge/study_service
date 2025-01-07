import traceback
import logging
from typing import List, Union, Literal
from pydantic import BaseModel
from .base_node import *
# from .study_arm import StudyArm
# from .study_epoch import StudyEpoch
# from .study_element import StudyElement
from uuid import uuid4

class StudyCell(NodeId):
  # studyArm: Union[StudyArm, None]
  # studyEpoch: Union[StudyEpoch, None]
  # studyElements: Union[List[StudyElement], None] = []
  instanceType: Literal['StudyCell']
  id: str
  uuid: str

  @classmethod
  def list(cls, uuid, page, size, filter):
    return cls.base_list("MATCH (m:StudyDesign {uuid: '%s'})-[]->(n:StudyCell)" % (uuid), "ORDER BY n.id ASC", page, size, filter)

  @classmethod
  def next_id(cls):
    study_cells = cls.base_list("MATCH (n:StudyCell)", "ORDER BY n.id ASC", page = 0, size = 1, filter = "")
    ids = [item['id'] for item in study_cells['items']]
    nums = [int(id.split('_')[-1]) for id in ids]
    m = max(nums)
    return "StudyCell_"+str(m+1) if m > 0 else "StudyCell_1" 

  @classmethod
  def create(cls):
    try:
      db = Neo4jConnection()
      with db.session() as session:
        next_id = cls.next_id()
        print("next_id",next_id)
        result = session.execute_write(cls._create_study_element, next_id)
        if not result:
          return {'error': "Failed to create study element, operation failed"}
        return result 
    except Exception as e:
      logging.error(f"Exception raised while creating study element")
      logging.error(f"Exception {e}\n{traceback.format_exc()}")
      return {'error': f"Exception. Failed to create study element"}

  @staticmethod
  def _create_study_element(tx, next_id):
    uuids = {'StudyCell': str(uuid4())}
    query = """
      CREATE (s:StudyCell {id: $s_id, uuid: $s_uuid1})
      set s.instanceType = 'StudyCell'
      set s.delete = 'me'
      RETURN s.uuid as uuid
    """
    print("query",query)
    result = tx.run(query, 
      s_id=next_id,
      s_uuid1=uuids['StudyCell']
    )
    for row in result:
      return uuids['StudyCell']
    return None

