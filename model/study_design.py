from typing import List, Union
from pydantic import BaseModel
from .base_node import *
from model.study_design_data_contract import StudyDesignDataContract
from model.study_design_subject_data import StudyDesignSubjectData
from model.study_design_sdtm import StudyDesignSDTM
from model.study_design_bc import StudyDesignBC
from model.code import Code
from model.study_cell import StudyCell
from model.study_epoch import StudyEpoch
from model.study_arm import StudyArm
from model.indication import Indication
from model.investigational_intervention import InvestigationalIntervention
from model.objective import Objective
from model.estimand import Estimand
from model.activity import Activity
from model.encounter import Encounter
from model.schedule_timeline import ScheduleTimeline
from model.population_definition import StudyDesignPopulation, StudyCohort
#from d4kms_service import Neo4jConnection

class StudyDesign(NodeNameLabelDesc):
  trialIntentTypes: List[Code] = []
  trialTypes: List[Code] = []
  # therapeuticAreas: List[Code] = []
  interventionModel: Code = None
  encounters: List[Encounter] = []
  activities: List[Activity] = []
  # biomedicalConcepts: List[BiomedicalConcept] = []
  # bcCategories: List[BiomedicalConceptCategory] = []
  # bcSurrogates: List[BiomedicalConceptSurrogate] = []
  # studyArms: List[StudyArm]
  studyCells: List[StudyCell] = []
  # studyDesignBlindingScheme: Union[AliasCode, None] = None
  # studyDesignRationale: str
  # studyEpochs: List[StudyEpoch]
  # studyElements: List[StudyElement] = []
  estimands: List[Estimand] = []
  indications: List[Indication] = []
  studyInterventions: List[InvestigationalIntervention] = []
  objectives: List[Objective] = []
  population: Union[StudyDesignPopulation, None] = None
  scheduleTimelines: List[ScheduleTimeline] = []
  # documentVersion: Union[StudyProtocolDocumentVersion, None] = None
  # studyEligibilityCritieria: List[EligibilityCriteria] = []    
  # dictionaries: List[SyntaxTemplateDictionary] = []

  @classmethod
  def list(cls, uuid, page, size, filter):
    return cls.base_list("MATCH (m:StudyVersion {uuid: '%s'})-[]->(n:StudyDesign)" % (uuid), "ORDER BY n.name ASC", page, size, filter)

#   def arms(self):
#     db = Neo4jConnection()
#     with db.session() as session:
#       return session.execute_read(self._arms, self.uuid)

#   def epochs(self):
#     db = Neo4jConnection()
#     with db.session() as session:
#       return session.execute_read(self._epochs, self.uuid)

#   def workflows(self):
#     db = Neo4jConnection()
#     with db.session() as session:
#       return session.execute_read(self._workflows, self.uuid)

  def summary(self):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})
        WITH sd
        MATCH (sd)-[:TRIAL_TYPES_REL]->(ttc:Code)
        MATCH (sd)-[:INTERVENTION_MODEL_REL]->(imc:Code)
        MATCH (sd)-[:TRIAL_INTENT_TYPES_REL]->(tic:Code)
        OPTIONAL MATCH (sd)-[:THERAPEUTIC_AREAS_REL]->(tac:Code)
        OPTIONAL MATCH (sd)-[:CHARACTERISTICS_REL]->(cc:Code)
        MATCH (sd)-[:POPULATION_REL]->(sdp:StudyDesignPopulation)
        OPTIONAL MATCH (sdp)-[:COHORTS_REL]->(coh:StudyCohort)
        RETURN sd, ttc, imc, tic, tac, cc, sdp, coh
      """ % (self.uuid)
      print(f"QUERY: {query}")
      result = None
      cohort_map = {}
      records = session.run(query)
      for record in records:
        if not result:
          result = StudyDesign.as_dict(record['sd'])
          result['trial_types'] = {}
          result['trial_intent'] = {}
          result['intervention_models'] = {}
          result['therapeutic_areas'] = {}
          result['characteristics'] = {}
          result['population'] = None
        sdp = StudyDesignPopulation.as_dict(record['sdp'])
        if not result['population']:
          result['population'] = sdp
          result['population']['cohorts'] = []
        if record['coh']:
          coh = StudyCohort.as_dict(record['coh'])
          if coh['uuid'] not in cohort_map:
            result['population']['cohorts'].append(coh)
            cohort_map[coh['uuid']] = True
        self._extract_code(record, 'ttc', result, 'trial_types')
        self._extract_code(record, 'imc', result, 'intervention_models')
        self._extract_code(record, 'tic', result, 'trial_intent')
        if record['tac']:
          self._extract_code(record, 'tac', result, 'therapeutic_areas')
        if record['cc']:
          self._extract_code(record, 'cc', result, 'characteristics')
    return result

  def design(self):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[:EPOCHS_REL]->(se1:StudyEpoch)
          WHERE NOT (se1)-[:PREVIOUS_REL]->()
        WITH se1,sd 
        MATCH path=(se1)-[:NEXT_REL *0..]->(se)
        WITH sd, se ORDER BY LENGTH(path) ASC
        MATCH (sd)-[:ARMS_REL]->(sa:StudyArm)
        RETURN sd, se, sa
      """ % (self.uuid)
      print(f"QUERY: {query}")
      results = {'study_design': None, 'study_epochs': [], 'study_arms': []}
      epochs = {}
      arms = {}
      records = session.run(query)
      for record in records:
        sd = StudyDesign.as_dict(record['sd'])
        if not results['study_design']:
          results['study_design'] = sd
        epoch = StudyEpoch.as_dict(record['se'])
        arm = StudyArm.as_dict(record['sa'])
        if epoch['uuid'] not in epochs:
          results['study_epochs'].append(epoch)
          epochs[epoch['uuid']] = True
        if arm['uuid'] not in arms:
          results['study_arms'].append(arm)
          arms[arm['uuid']] = True
    print(f"RESULTS: {results}")
    return results

  def _extract_code(self, record, record_key, result, result_key):
    code = Code.as_dict(record[record_key])
    result[result_key][code['decode']] = code['decode']

  def data_contract(self, page, size, filter):
    return StudyDesignDataContract.read(self.uuid, page, size, filter)

  def subject_data(self, page, size, filter):
    return StudyDesignSubjectData.read(self.uuid, page, size, filter)

  def sdtm_domains(self, page, size, filter):
    return StudyDesignSDTM.domains(self.uuid, page, size, filter)

  def biomedical_concepts_unlinked(self, page, size, filter):
    return StudyDesignBC.unlinked(self.uuid, page, size, filter)

#   @staticmethod
#   def _create_workflow(tx, uuid, name, description):
#       query = (
#         "MATCH (sd:StudyDesign { uuid: $uuid1 })-[:STUDY_WORKFLOW]->(e)"
#         "WHERE NOT (e)-[:NEXT_WORKFLOW]->()"
#         "CREATE (e1:Workflow { workflowName: $name, workflowDesc: $desc, uuid: $uuid2 })"
#         "CREATE (e)-[:NEXT_WORKFLOW]->(e1)"
#         "CREATE (e1)-[:PREVIOUS_WORKFLOW]->(e)"
#         "CREATE (sd)-[:STUDY_WORKFLOW]->(e1)"
#         "RETURN e1.uuid as uuid"
#       )
#       result = tx.run(query, name=name, desc=description, uuid1=uuid, uuid2=str(uuid4()))
# #      try:
#       for row in result:
#         return row["uuid"]
#       return None
# #      except ServiceUnavailable as exception:
# #        logging.error("{query} raised an error: \n {exception}".format(
# #          query=query, exception=exception))
# #        raise

#   @staticmethod
#   def _arms(tx, uuid):
#     results = []
#     query = (
#       "MATCH (s:StudyDesign {uuid: $uuid})-[:STUDY_CELL]->(:StudyCell)-[:STUDY_ARM]->(a:StudyArm)"
#       "RETURN DISTINCT a ORDER BY a.name"
#     )
#     result = tx.run(query, uuid=uuid)
#     for row in result:
#       results.append(StudyArm.wrap(row['a']))
#     return results

#   @staticmethod
#   def _epochs(tx, uuid):
#     results = []
#     query = (
#       "MATCH (s:StudyDesign {uuid: $uuid})-[:STUDY_CELL]->(:StudyCell)-[:STUDY_EPOCH]->(e:StudyEpoch)"
#       "RETURN DISTINCT e ORDER BY e.name"
#     )
#     result = tx.run(query, uuid=uuid)
#     for row in result:
#       results.append(StudyEpoch.wrap(row['e']))
#     return results

#   @staticmethod
#   def _workflows(tx, uuid):
#     results = []
#     query = (
#       "MATCH (s:StudyDesign {uuid: $uuid})-[:STUDY_WORKFLOW]->(wfi:Workflow)"
#       "RETURN wfi"
#     )
#     result = tx.run(query, uuid=uuid)
#     for row in result:
#       results.append(Workflow.wrap(row['wfi']))
#     return results

#   @staticmethod
#   def _create_first_workflow(tx, uuid, name, description):
#     query = (
#       "MATCH (sd:StudyDesign { uuid: $uuid1 })"
#       "CREATE (e:Encounter { encounterName: $name, encounterDesc: $desc, uuid: $uuid2 })"
#       "CREATE (sd)-[:STUDY_WORKFLOW]->(e)"
#       "RETURN e.uuid as uuid"
#     )
#     result = tx.run(query, name=name, desc=description, uuid1=uuid, uuid2=str(uuid4()))
#     for row in result:
#       return row["uuid"]
#     return None

#   @staticmethod
#   def _exists(tx, uuid, name):
#     query = (
#       "MATCH (ep:StudyDesign { uuid: $uuid })-[:STUDY_WORKFLOW]->(e:Encounter { encounterName: $name })"
#       "RETURN e.uuid as uuid"
#     )
#     result = tx.run(query, name=name, uuid=uuid)
#     if result.peek() == None:
#       return False
#     else:
#       return True

#   @staticmethod
#   def _any(tx, uuid):
#     query = (
#       "MATCH (sd:StudyDesign { uuid: $uuid })-[:STUDY_WORKFLOW]->(e:Encounter)"
#       "RETURN e.uuid"
#     )
#     result = tx.run(query, uuid=uuid)
#     if result.peek() == None:
#       return False
#     else:
#       return True

