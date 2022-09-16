from typing import List, Union
from pydantic import BaseModel
from model.node import Node
from model.study_design_views import StudyDesignViews
from .code import Code
from .study_cell import StudyCell
from .indication import Indication
from .investigational_intervention import InvestigationalIntervention
from .study_design_population import StudyDesignPopulation
from .objective import Objective
from .workflow import Workflow
from .estimand import Estimand
from .activity import Activity
from .encounter import Encounter
from .neo4j_connection import Neo4jConnection
from uuid import UUID, uuid4

class StudyDesignOut(BaseModel):
  uuid: str

class ChildList(BaseModel):
  items: List[str]
  page: int
  size: int
  filter: str
  count: int

class StudyDesign(Node):
  uuid: str
  trialIntentTypes: Union[List[Code], List[UUID]] = []
  trialType: Union[Code, UUID, None] 
  interventionModel: Union[Code, UUID, None]
  studyCells: Union[List[StudyCell], List[UUID], None] = []
  studyIndications: Union[List[Indication], List[UUID], None] = []
  studyInvestigationalInterventions: Union[List[InvestigationalIntervention], List[UUID], None] = []
  studyPopulations: Union[List[StudyDesignPopulation], List[UUID], None] = []
  studyObjectives: Union[List[Objective], List[UUID], None] = []
  studyWorkflows: Union[List[Workflow], List[UUID], None] = []
  studyEstimands: Union[List[Estimand], List[UUID], None] = []
  studyActivities: Union[List[Activity], List[UUID], None] = []   # EXTENSION
  studyEncounters: Union[List[Encounter], List[UUID], None] = []  # EXTENSION

  @classmethod
  def create(cls, uuid, name, description):
    db = Neo4jConnection()
    with db.session() as session:
      if session.execute_read(cls._any, uuid):
        if not session.execute_read(cls._exists, uuid, name):
          return session.execute_write(cls._create_workflow, uuid, name, description)
        else:
          return None
      else:
        return session.execute_write(cls._create_first_workflow, uuid, name, description)

  def epochs(self):
    db = Neo4jConnection()
    with db.session() as session:
      results = {'items': [], 'page': 1, 'size': 0, 'filter': "", 'count': 0 }
      results['items'] = session.execute_read(self._epochs, self.uuid)
      results['count'] = len(results['items'])
      results['size'] = len(results['items'])
      return results

  def workflows(self):
    db = Neo4jConnection()
    with db.session() as session:
      results = {'items': [], 'page': 1, 'size': 0, 'filter': "", 'count': 0 }
      results['items'] = session.execute_read(self._workflows, self.uuid)
      results['count'] = len(results['items'])
      results['size'] = len(results['items'])
      return results

  def soa(self):
    return StudyDesignViews().soa(self.uuid)

  def data_contract(self):
    return StudyDesignViews().data_contract(self.uuid)

  @staticmethod
  def _create_workflow(tx, uuid, name, description):
      query = (
        "MATCH (sd:StudyDesign { uuid: $uuid1 })-[:STUDY_WORKFLOW]->(e)"
        "WHERE NOT (e)-[:NEXT_workflow]->()"
        "CREATE (e1:Workflow { workflowName: $name, workflowDesc: $desc, uuid: $uuid2 })"
        "CREATE (e)-[:NEXT_workflow]->(e1)"
        "CREATE (e1)-[:PREVIOUS_workflow]->(e)"
        "CREATE (sd)-[:STUDY_WORKFLOW]->(e1)"
        "RETURN e1.uuid as uuid"
      )
      result = tx.run(query, name=name, desc=description, uuid1=uuid, uuid2=str(uuid4()))
#      try:
      for row in result:
        return row["uuid"]
      return None
#      except ServiceUnavailable as exception:
#        logging.error("{query} raised an error: \n {exception}".format(
#          query=query, exception=exception))
#        raise

  @staticmethod
  def _epochs(tx, uuid):
    results = []
    query = (
      "MATCH (s:StudyDesign {uuid: $uuid})-[:STUDY_CELL]->(:StudyCell)-[:STUDY_EPOCH]->(e:StudyEpoch)"
      "RETURN e.uuid as uuid"
    )
    result = tx.run(query, uuid=uuid)
    for row in result:
      results.append(row['uuid'])
    return results

  @staticmethod
  def _workflows(tx, uuid):
    results = []
    query = (
      "MATCH (s:StudyDesign {uuid: $uuid})-[:STUDY_WORKFLOW]->(wfi:Workflow)"
      "RETURN wfi.uuid as uuid"
    )
    result = tx.run(query, uuid=uuid)
    for row in result:
      results.append(row['uuid'])
    return results

  @staticmethod
  def _create_first_workflow(tx, uuid, name, description):
    query = (
      "MATCH (sd:StudyDesign { uuid: $uuid1 })"
      "CREATE (e:Encounter { encounterName: $name, encounterDesc: $desc, uuid: $uuid2 })"
      "CREATE (sd)-[:STUDY_WORKFLOW]->(e)"
      "RETURN e.uuid as uuid"
    )
    result = tx.run(query, name=name, desc=description, uuid1=uuid, uuid2=str(uuid4()))
    for row in result:
      return row["uuid"]
    return None

  @staticmethod
  def _exists(tx, uuid, name):
    query = (
      "MATCH (ep:StudyDesign { uuid: $uuid })-[:STUDY_WORKFLOW]->(e:Encounter { encounterName: $name })"
      "RETURN e.uuid as uuid"
    )
    result = tx.run(query, name=name, uuid=uuid)
    if result.peek() == None:
      return False
    else:
      return True

  @staticmethod
  def _any(tx, uuid):
    query = (
      "MATCH (sd:StudyDesign { uuid: $uuid })-[:STUDY_WORKFLOW]->(e:Encounter)"
      "RETURN e.uuid"
    )
    result = tx.run(query, uuid=uuid)
    if result.peek() == None:
      return False
    else:
      return True

