from typing import List, Union
from pydantic import BaseModel
from model.node import Node
from model.neo4j_connection import Neo4jConnection
from .workflow_item import WorkflowItem
from uuid import UUID, uuid4

class WorkflowIn(BaseModel):
  name: str # EXTENSION
  description: str

class Workflow(Node):
  uuid: Union[UUID, None] = None
  workflowName: str # EXTENSION
  workflowDesc: str
  workflowItems: Union[List[WorkflowItem], List[UUID], None] = []
  uniqueLabel: str = ""

  def add_workflow_item(self, description, encounter_uuid, activity_uuid):
    db = Neo4jConnection()
    with db.session() as session:
      count = session.execute_read(self._wfi_count, str(self.uuid))
      return session.execute_write(self._add_workflow_item, str(self.uuid), description, encounter_uuid, activity_uuid, count, self.uniqueLabel)

  @staticmethod
  def _add_workflow_item(tx, uuid, description, encounter_uuid, activity_uuid, count, prefix):
    unique_label = "%s-wfi-%s" % (prefix, count + 1)
    query = (
      "MATCH (wf:Workflow {uuid: $uuid1}), (e:Encounter {uuid: $uuid2}), (a:Activity {uuid: $uuid3})"
      "CREATE (wfi:WorkflowItem {workflowItemDesc: $desc, uuid: $uuid4, uniqueLabel: $unique_label})"
      "CREATE (wf)-[:WORKFLOW_ITEM]->(wfi)"
      "CREATE (wfi)-[:WORKFLOW_ITEM_ENCOUNTER]->(e)"
      "CREATE (wfi)-[:WORKFLOW_ITEM_ACTIVITY]->(a)"
      "RETURN wfi.uuid as uuid"
    )
    result = tx.run(query, uuid1=uuid, uuid2=encounter_uuid, uuid3=activity_uuid, desc=description, 
      uuid4=str(uuid4()), unique_label=unique_label
    )
    for row in result:
      return row['uuid']
    return None

  @staticmethod
  def _wfi_count(tx, uuid):
    query = (
      "MATCH (wf:Workflow {uuid: $uuid1})-[:WORKFLOW_ITEM]->(wfi)"
      "RETURN COUNT(wfi) AS count"
    )
    result = tx.run(query, uuid1=uuid)
    for row in result:
      return row['count']
    return 0
