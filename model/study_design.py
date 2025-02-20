import traceback
import logging
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
from model.study_define import StudyDefine
from model.study_form import StudyForm
from model.domains_trial_design import TrialDesignDomain
#from d4kms_service import Neo4jConnection
from uuid import uuid4

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
  def create(cls, name, description, label):
    try:
      db = Neo4jConnection()
      with db.session() as session:
        next_id = cls.next_id()
        result = session.execute_write(cls._create_study_design, next_id, name, description, label)
        if not result:
          return {'error': "Failed to create study, operation failed"}
        return result 
    except Exception as e:
      logging.error(f"Exception raised while creating study")
      logging.error(f"Exception {e}\n{traceback.format_exc()}")
      return {'error': f"Exception. Failed to create study"}

  @classmethod
  def list(cls, uuid, page, size, filter):
    return cls.base_list("MATCH (m:StudyVersion {uuid: '%s'})-[]->(n:StudyDesign)" % (uuid), "ORDER BY n.name ASC", page, size, filter)

  @classmethod
  def list_with_source(cls, uuid, page, size, filter):
    return cls._list_with_pdf_source(cls, uuid, page, size, filter)

  @classmethod
  def delete(cls, uuid):
    count = cls._delete_study_design(uuid)
    return {'message':'Deleted study design '+uuid, 'count': count}

  @classmethod
  def next_id(cls):
    study_designs = cls.base_list("MATCH (n:StudyDesign)", "ORDER BY n.id ASC", page = 0, size = 1, filter = "")
    ids = [item['id'] for item in study_designs['items']]
    nums = [int(id.split('_')[-1]) for id in ids]
    m = max(nums)
    return "StudyDesign_"+str(m+1) if m > 0 else "StudyDesign_1" 

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

  def _list_with_pdf_source(self, uuid, page, size, filter):
    db = Neo4jConnection()
    with db.session() as session:
      result = {}
      query = """
        MATCH (m:StudyVersion {uuid: '%s'})-[]->(sd:StudyDesign)
        RETURN count(sd) as count
      """ % (uuid)
      records = session.run(query)
      for record in records.data():
        result['count'] = record['count']

      result['items'] = []
      query = """
        MATCH (m:StudyVersion {uuid: '%s'})-[]->(sd:StudyDesign)
        OPTIONAL MATCH (sd)-->(pdf:PdfFile)
        RETURN sd.name as name, sd.uuid as uuid, pdf.uuid as pdf_uuid, pdf.chat_json is not null as chat_json, pdf.usdm_json is not null as usdm_json
        ORDER BY sd.name ASC
        SKIP 0 LIMIT 10
      """ % (uuid)
      # print(f"QUERY: {query}")
      records = session.run(query)
      for record in records.data():
        result['items'].append(record)
    return result

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

  def subjects(self, page, size, filter):
    return StudyDesignSubjectData.subjects(self.uuid, page, size, filter)

  def sdtm_domains(self, page, size, filter):
    return StudyDesignSDTM.domains(self.uuid, page, size, filter)

  def sdtm_trial_design_domain(self, domain_name, page, size, filter):
    return TrialDesignDomain.create(self.uuid, domain_name, page, size, filter)

  def sdtm_define(self, page, size, filter):
    return StudyDefine.make_define(self.uuid, page, size, filter)

  def study_form(self, page, size, filter):
    return StudyForm.make_form(self.uuid, page, size, filter)

  def study_design_timelines(self, page, size, filter):
    return ScheduleTimeline.list(self.uuid, page, size, filter)
    # return Encounter.list(self.uuid, page, size, filter)

  # def study_design_encounters(self, page, size, filter):
  #   return Encounter.list(self.uuid, page, size, filter)
  #   # return Encounter.list(self.uuid, page, size, filter)

  def lab_transfer_spec(self, page, size, filter):
    return StudyForm.make_lab_transfer_spec(self.uuid, page, size, filter)

  @staticmethod
  def datapoint_form(datapoint, page, size, filter):
    return StudyForm.datapoint_form(datapoint, page, size, filter)

  @staticmethod
  def data_contract_specification(uri, page, size, filter):
    items = StudyDesign._data_contract_specification(uri, page, size, filter)
    result = {'items': items, 'source_data_contract': uri, 'page': page, 'size': size, 'filter': filter, 'count': 1 }
    return result

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

  @staticmethod
  def _create_study_design(tx, next_id, name, description, label):
    uuids = {'StudyDesign': str(uuid4())}
    query = """
      CREATE (s:StudyDesign {id: $s_id, name: $s_name, description: $s_description, label: $s_label, uuid: $s_uuid1, instanceType: 'Study'})
      set s.delete = 'me'
      RETURN s.uuid as uuid
    """
    result = tx.run(query, 
      s_id=next_id,
      s_name=name, 
      s_description=description, 
      s_label=label, 
      s_uuid1=uuids['StudyDesign']
    )
    for row in result:
      return uuids['StudyDesign']
    return None

  @staticmethod
  def _delete_study_design(uuid):
    count = 0
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign { uuid: '%s' })
        OPTIONAL MATCH (sd)-[]->(a)
        WHERE a.delete = 'me'
        OPTIONAL MATCH (a)-[]-(b)
        WHERE b.delete = 'me'
        detach delete sd, a, b
        RETURN count(*) as count
      """ % (uuid)
      # print("query",query)
      result = session.run(query)
      for record in result.data():
        count = record['count']
    db.close()
    return count

  @staticmethod
  def _data_contract_specification(uri, page, size, filter):
    db = Neo4jConnection()
    results = []
    with db.session() as session:
      query = """
        // Get data contract context
        MATCH (dc:DataContract { uri: '%s' })
        with dc
        match (dc)-->(bcp:BiomedicalConceptProperty)<--(bc:BiomedicalConcept)
        match (dc)-[:INSTANCES_REL]->(sai:ScheduledActivityInstance)
        match (sai)-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t_from:Timing)
        match (t_from)-[:RELATIVE_TO_FROM_REL]->()
        match (t_from)<-[:TIMINGS_REL]-(tl:ScheduleTimeline)
        match (t_from)-[:RELATIVE_TO_FROM_REL]->(rel:Code)
        match (t_from)-[:TYPE_REL]->(type:Code)
        // optional match (sai)-[:RELATIVE_TO_SCHEDULED_INSTANCE_REL]-(t_to:Timing)
        optional match (t_from)<-[:SCHEDULED_AT_REL]-(e:Encounter)
        optional match (bcp)-[:RESPONSE_CODES_REL]->(:ResponseCode)-[:CODE_REL]->(term:Code)
        return bc.name as bc_name, bcp.generic_name as bcp_name, dc.uri as uri, t_from.valueLabel as timing_valueLabel, t_from.label as timing_label, e.label as encounter_label, collect(term.pref_label) as terms
      """ % (uri)
      print("dc context query",query)
      result = session.run(query)
      for record in result.data():
        results.append(record)
    db.close()
    return results
