from typing import Union, List, Literal
from .base_node import *
from .code import Code
from .range import Range
from .eligibility_criterion import EligibilityCriterion
from .characteristic import Characteristic

class PopulationDefinition(NodeNameLabelDesc):
  includesHealthySubjects: bool
  plannedEnrollmentNumber: Union[Range, None] = None
  plannedCompletionNumber: Union[Range, None] = None
  plannedSex: List[Code] = []
  criteria: List[EligibilityCriterion] = []
  plannedAge: Union[Range, None] = None
  instanceType: Literal['PopulationDefinition']

class StudyCohort(PopulationDefinition):
  characteristics: List[Characteristic] = []
  instanceType: Literal['StudyCohort']

  def summary(self):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (pd:StudyCohort {uuid: '%s'})
        WITH pd
        OPTIONAL MATCH (pd)-[:PLANNED_ENROLLMENT_NUMBER_REL]->(prn)
        OPTIONAL MATCH (pd)-[:PLANNED_COMPLETION_NUMBER_REL]->(pcn)
        OPTIONAL MATCH (pd)-[:PLANNED_SEX_REL]->(ps)
        OPTIONAL MATCH (pd)-[:PLANNED_AGE_REL]->(pa)
        OPTIONAL MATCH (pd)-[:CHARACTERISTICS_REL]->(c)
        OPTIONAL MATCH (pd)-[:CRITERIA_REL]->(cr)-[:CATEGORY_REL]->(crc:Code)
        RETURN pd, prn, pcn, ps, pa, c, cr, crc
      """ % (self.uuid)
      print(f"QUERY: {query}")
      result = None
      sex_map = {}
      characteristic_map = {}
      criteria_map = {}
      records = session.run(query)
      for record in records:
        pd = StudyDesignPopulation.as_dict(record['pd'])
        if not result:
          result = pd
          result['planned_sex'] = []
          result['characteristics'] = []
          result['criteria'] = {}
        if record['prn']:
          result['planned_enrollment'] = Range.as_dict(record['prn'])
        if record['pcn']:
          result['planned_completion'] = Range.as_dict(record['pcn'])
        if record['ps']:
          ps = Code.as_dict(record['ps'])
          if ps['uuid'] not in sex_map:
            result['planned_sex'].append(ps)
            sex_map[ps['uuid']] = True
        if record['pa']:
          result['planned_age'] = Range.as_dict(record['pa'])
        if record['c']:
          c = Characteristic.as_dict(record['c'])
          if c['uuid'] not in characteristic_map:
            result['characteristic'].append(c)
            characteristic_map[c['uuid']] = True
        if record['cr']:
          cr = EligibilityCriterion.as_dict(record['cr'])
          if cr['uuid'] not in criteria_map:
            code = Code.as_dict(record['crc'])
            cr['category'] = code
            key = code['decode'].replace(' ', '_').lower()
            if key not in result['criteria']:
              result['criteria'][key] = []
            result['criteria'][key].append(cr)
            criteria_map[cr['uuid']] = True
    print(f"RESULT: {result}")
    for k, v in result['criteria'].items():
      result['criteria'][k] = sorted(v, key=lambda x: x['identifier'])
    print(f"RESULT: {result}")
    return result

class StudyDesignPopulation(PopulationDefinition):
  cohorts: List[StudyCohort] = []
  instanceType: Literal['StudyDesignPopulation']

  def summary(self):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (pd:StudyDesignPopulation {uuid: '%s'})
        WITH pd
        OPTIONAL MATCH (pd)-[:PLANNED_ENROLLMENT_NUMBER_REL]->(prn)
        OPTIONAL MATCH (pd)-[:PLANNED_COMPLETION_NUMBER_REL]->(pcn)
        OPTIONAL MATCH (pd)-[:PLANNED_SEX_REL]->(ps)
        OPTIONAL MATCH (pd)-[:PLANNED_AGE_REL]->(pa)
        OPTIONAL MATCH (pd)-[:COHORTS_REL]->(coh:StudyCohort)
        OPTIONAL MATCH (pd)-[:CRITERIA_REL]->(cr)-[:CATEGORY_REL]->(crc:Code)
        RETURN pd, prn, pcn, ps, pa, coh, cr, crc
      """ % (self.uuid)
      print(f"QUERY: {query}")
      result = None
      sex_map = {}
      cohort_map = {}
      criteria_map = {}
      records = session.run(query)
      for record in records:
        pd = StudyDesignPopulation.as_dict(record['pd'])
        if not result:
          result = pd
          result['planned_sex'] = []
          result['cohorts'] = []
          result['criteria'] = {}
        if record['prn']:
          result['planned_enrollment'] = Range.as_dict(record['prn'])
        if record['pcn']:
          result['planned_completion'] = Range.as_dict(record['pcn'])
        if record['ps']:
          ps = Code.as_dict(record['ps'])
          if ps['uuid'] not in sex_map:
            result['planned_sex'].append(ps)
            sex_map[ps['uuid']] = True
        if record['coh']:
          coh = StudyCohort.as_dict(record['coh'])
          if coh['uuid'] not in cohort_map:
            result['cohorts'].append(coh)
            cohort_map[coh['uuid']] = True
        if record['pa']:
          result['planned_age'] = Range.as_dict(record['pa'])
        if record['cr']:
          cr = EligibilityCriterion.as_dict(record['cr'])
          if cr['uuid'] not in criteria_map:
            code = Code.as_dict(record['crc'])
            cr['category'] = code
            key = code['decode'].replace(' ', '_').lower()
            if key not in result['criteria']:
              result['criteria'][key] = []
            result['criteria'][key].append(cr)
            criteria_map[cr['uuid']] = True
    print(f"RESULT: {result}")
    for k, v in result['criteria'].items():
      result['criteria'][k] = sorted(v, key=lambda x: x['identifier'])
    print(f"RESULT: {result}")
    return result

