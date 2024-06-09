from typing import List, Union
from pydantic import BaseModel
from .base_node import *
from model.study_design_data_contract import StudyDesignDataContract
from model.study_design_subject_data import StudyDesignSubjectData
from model.study_design_sdtm import StudyDesignSDTM
from model.study_design_bc import StudyDesignBC
from model.code import Code
from model.study_cell import StudyCell
#from model.study_epoch import StudyEpoch
#from model.study_arm import StudyArm
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
        MATCH (sd)-[:THERAPEUTIC_AREAS_REL]->(tac:Code)
        MATCH (sd)-[:CHARACTERISTICS_REL]->(cc:Code)
        MATCH (sd)-[:POPULATION_REL]->(sdp:StudyDesignPopulation)
        OPTIONAL MATCH (sdp)-[:COHORTS_REL]->(coh:Cohort)
        RETURN sd, ttc, imc, tic, tac, cc, sdp, coh
      """ % (self.uuid)
      print(f"QUERY: {query}")
      results = {}
      records = session.run(query)
      for record in records:
        sd = StudyDesign.as_dict(record['sd'])
        if sd['uuid'] not in results:
          result = sd
          result['trial_types'] = {}
          result['trial_intent'] = {}
          result['intervention_models'] = {}
          result['therapeutic_areas'] = {}
          result['characteristics'] = {}
          results[sd['uuid']] = result
          result['population'] = None
        else:
          result = results[sd['uuid']]
        sdp = StudyDesignPopulation.as_dict(record['sdp'])
        if not result['population']:
          result['population'] = sdp
          result['population']['cohorts'] = {}
        if 'coh' in record:
          coh = StudyCohort.as_dict(record['coh'])
          result['population']['cohorts'][coh['uuid']] = coh
        self._extract_code(record, 'ttc', result, 'trial_types')
        self._extract_code(record, 'imc', result, 'intervention_models')
        self._extract_code(record, 'tic', result, 'trial_intent')
        self._extract_code(record, 'tac', result, 'therapeutic_areas')
        self._extract_code(record, 'cc', result, 'characteristics')

    print(f"RESULTS: {list(results.values())}")
    return list(results.values())[0]

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

