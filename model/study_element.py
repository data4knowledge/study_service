import traceback
import logging
from typing import Union, List, Literal
from .base_node import *
from .transition_rule import TransitionRule
from uuid import uuid4

class StudyElement(NodeNameLabelDesc):
  name: str
  description: str
  label: str
  # transitionStartRule: Union[TransitionRule, None] = None
  # transitionEndRule: Union[TransitionRule, None] = None
  # studyInterventionIds: List[str] = []
  instanceType: Literal['StudyElement']

  @classmethod
  def list(cls, uuid, page, size, filter):
    return cls.base_list("MATCH (m:StudyDesign {uuid: '%s'})-[]->(n:StudyElement)" % (uuid), "ORDER BY n.id ASC", page, size, filter)

  @classmethod
  def next_id(cls):
    study_elements = cls.base_list("MATCH (n:StudyElement)", "ORDER BY n.id ASC", page = 0, size = 1, filter = "")
    ids = [item['id'] for item in study_elements['items']]
    nums = [int(id.split('_')[-1]) for id in ids]
    m = max(nums)
    return "StudyElement_"+str(m+1) if m > 0 else "StudyElement_1" 


  @classmethod
  def create(cls, name, description, label):
    try:
      db = Neo4jConnection()
      with db.session() as session:
        next_id = cls.next_id()
        result = session.execute_write(cls._create_study_element, next_id, name, description, label)
        if not result:
          return {'error': "Failed to create study element, operation failed"}
        return result 
    except Exception as e:
      logging.error(f"Exception raised while creating study element")
      logging.error(f"Exception {e}\n{traceback.format_exc()}")
      return {'error': f"Exception. Failed to create study element"}

  @staticmethod
  def _create_study_element(tx, next_id, name, description, label):
    uuids = {'StudyElement': str(uuid4())}
    query = """
      CREATE (s:StudyElement {id: $s_id, uuid: $s_uuid1})
      set s.name = $s_name
      set s.description = $s_description
      set s.label = $s_label
      set s.instanceType = 'StudyElement'
      set s.delete = 'me'
      RETURN s.uuid as uuid
    """
    # print("query",query)
    result = tx.run(query, 
      s_id=next_id, 
      s_name=name, 
      s_description=description, 
      s_label=label, 
      s_uuid1=uuids['StudyElement']
    )
    for row in result:
      return uuids['StudyElement']
    return None

