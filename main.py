import os
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, status, Request, BackgroundTasks
from fastapi.responses import FileResponse
from model.crm import CRM
from model.system import SystemOut
from model.study_file import StudyFile
from model.data_file import DataFile
from model.study import Study
from model.study_version import StudyVersion
from model.study_design import StudyDesign
from d4kms_service import Neo4jConnection
from model.schedule_timeline import ScheduleTimeline
from model.study_arm import StudyArm
from model.activity import Activity
from model.study_protocol_document_version import StudyProtocolDocumentVersion, SPDVBackground
# from model.study_identifier import StudyIdentifier, StudyIdentifierIn
# from model.study_design import StudyDesign
from model.domain import Domain
# from model.activity import Activity, ActivityIn
from model.study_epoch import StudyEpoch #, StudyEpochIn
from model.study_arm import StudyArm #, StudyArmIn
from model.study_cell import StudyCell
from model.study_element import StudyElement
# from model.study_data import StudyData, StudyDataIn
from model.encounter import Encounter #, EncounterIn, EncounterLink
from model.scheduled_instance import ScheduledActivityInstance
from typing import List
from model.population_definition import StudyDesignPopulation, StudyCohort
from model.template.template_manager import template_manager
from d4kms_generic import ServiceEnvironment
from d4kms_generic import application_logger

VERSION = "0.14"
SYSTEM_NAME = "d4k Study Microservice"

app = FastAPI(
  title = SYSTEM_NAME,
  description = "A microservice to handle Study Builds in a Neo4j database.",
  version = VERSION
)

# FOR DEBUGGING
se = ServiceEnvironment()

@app.get("/", 
  summary="Get system and version",
  description="Returns the microservice system details and the version running.", 
  response_model=SystemOut)
@app.get("/v1", 
  summary="Get system and version",
  description="Returns the microservice system details and the version running.", 
  response_model=SystemOut)
async def read_root():
  return SystemOut(**{ 'system_name': SYSTEM_NAME, 'version': VERSION, 'environment': ServiceEnvironment().environment() })

@app.get("/ping", 
  summary="Check service and DB",
  description="A simple microservice healthcheck that counts node in the database to check connectivity.", 
  response_model=dict)
@app.get("/v1/ping", 
  summary="Check service and DB",
  description="A simple microservice healthcheck that counts node in the database to check connectivity.", 
  response_model=dict)
def ping():
  return { 'count': Neo4jConnection().count() }

@app.delete("/v1/clean", 
  summary="Delete database",
  description="Deletes the entire database.",
  status_code=204)
async def delete_clean(background_tasks: BackgroundTasks):
  db = Neo4jConnection()
  db.clear()
  crm = CRM()
  background_tasks.add_task(crm.execute)
  background_tasks.add_task(crm.add_crm_nodes)

@app.get("/v1/test", response_model=dict)
@app.get("/test", response_model=dict)
async def test(request: Request, name: str):
  from model.study_design_bc import StudyDesignBC
  return StudyDesignBC.fix(name)

# Study Files
# ===========

@app.get("/v1/studyFiles", 
  summary="Get study files",
  description="Get the list of study files",
  response_model=dict)
async def list_study_files(page: int = 0, size: int = 0, filter: str=""):
  return StudyFile.list(page, size, filter)

@app.post('/v1/studyFiles', 
  summary="Load a study",
  description="Upload and process a study Excel file loading the data into the database", 
  status_code=status.HTTP_201_CREATED,
  response_model=str)
async def create_study_file(request: Request, background_tasks: BackgroundTasks):
  form = await request.form()
  filename = form['upload_file'].filename
  contents = await form['upload_file'].read()
  sf = StudyFile()
  success = sf.create(filename, contents)
  if success:
    background_tasks.add_task(sf.execute)
    return sf.uuid
  else:
    raise HTTPException(status_code=409, detail=f"Failed to upload the file. {sf.error}")

@app.get("/v1/studyFiles/{uuid}/status", 
  summary="Get study file status",
  description="Get the status of the processing for a given study file",
  response_model=dict)
async def get_study_file_status(uuid: str):
  sf = StudyFile.find(uuid)
  if sf:
    return sf.get_status()
  else:
    raise HTTPException(status_code=404, detail="The requested study file cannot be found")

@app.get("/v1/studyFiles/{uuid}/load/{name}", 
  summary="Download a study load file",
  description="Download the specified study load file")
async def get_study_file(uuid: str, name: str):
  full_path = os.path.join('uploads', uuid, name)
  return FileResponse(path=full_path, filename=name, media_type='text/plain')

# Templates
# =========

@app.get("/v1/templates", 
  summary="Get templates",
  description="Get the set of available templates",
  response_model=list)
async def get_temlates():
  return template_manager.templates()
  
# Studies
# =======

@app.get("/v1/studies", 
  summary="List of studies",
  description="Provide a list of all studies.",
  status_code=200,
  response_model=dict)
async def list_studies(page: int = 0, size: int = 0, filter: str=""):
  return Study.list(page, size, filter)

@app.get("/v1/studies/phase", 
  summary="Count of all study phases",
  description="Provide a count of all study phases.",
  status_code=200,
  response_model=list)
async def study_phase():
  return Study.phase()

@app.get("/v1/studies/{uuid}/summary", 
  summary="Get study summary",
  description="Provide the summary details for a single study",
  status_code=200,
  response_model=list)
async def study_summary(uuid: str):
  study = Study.find(uuid)
  if study:
    return study.summary()
  else:
    raise HTTPException(status_code=404, detail="The requested study cannot be found")

@app.post("/v1/studies", 
  summary="Create a new study",
  description="Creates a study. If succesful the uuid of the created resource is returned.",
  status_code=201,
  response_model=str)
async def create_study(name: str, background_tasks: BackgroundTasks, description: str="", label: str="", template: str=""):
  #print(f"TEMPLATE: {template}")
  result = Study.create(name, description, label, template)
  if not 'error' in result:
    sv = StudyVersion.find(result['StudyVersion'])
    background_tasks.add_task(SPDVBackground(sv).add_all_sections, result, template)
    return result['Study']
  else:
    raise HTTPException(status_code=409, detail=result['error'])

# Study Versions
# ==============
  
@app.get("/v1/studies/{uuid}/studyVersions", 
  summary="Get the study versions for a study",
  description="Provides a list of study versions that exisit for a specified study.",
  response_model=dict)
async def list_study_versions(request: Request, page: int = 0, size: int = 0, filter: str=""):
  uuid = request.path_params['uuid']
  return StudyVersion.list(uuid, page, size, filter)

@app.get("/v1/studyVersions/{uuid}", 
  summary="Get a study versions",
  description="Provides a study versions",
  response_model=StudyVersion)
async def study_version(request: Request, uuid: str):
  version = StudyVersion.find(uuid)
  if version:
    return version
  else:
    raise HTTPException(status_code=404, detail="The requested study version cannot be found")

@app.get("/v1/studyVersions/{uuid}/summary", 
  summary="Get a study version summary",
  description="Provides a study version summary",
  response_model=dict)
async def study_version_summary(request: Request, uuid: str):
  version = StudyVersion.find(uuid)
  if version:
    return version.summary()
  else:
    raise HTTPException(status_code=404, detail="The requested study version cannot be found")

# Protocol Document Versions
# ==========================

@app.get("/v1/studyVersions/{uuid}/protocolDocument", 
  summary="Get the protocol document for a study version",
  description="Get the protococl document for a study version",
  response_model=str)
async def get_protocol_document(uuid: str):
  study_version = StudyVersion.find(uuid)
  if study_version:
    result = study_version.protocol_document()
    if not 'error' in result:
      return result['uuid']
    else:
      raise HTTPException(status_code=404, detail=result['error'])
  else:
    raise HTTPException(status_code=404, detail="The requested study version cannot be found")

@app.get("/v1/protocolDocumentVersions/{uuid}/sectionList", 
  summary="Get the protocol document version section list",
  description="Get the protococl document section list for a study.",
  response_model=dict)
async def get_protocol_section_list(uuid: str):
  result = StudyProtocolDocumentVersion.find(uuid)
  if not 'error' in result:
    return result.section_list()
  else:
    raise HTTPException(status_code=404, detail="The requested protocol document version cannot be found")

@app.get("/v1/protocolDocumentVersions/{uuid}/documentDefinition", 
  summary="Get the protocol document version definition",
  description="Get the protococl document definition",
  response_model=dict)
async def get_document_definition(uuid: str):
  doc = StudyProtocolDocumentVersion.find(uuid)
  if not 'error' in doc:
    result = doc.document_definition()
    return result
  else:
    raise HTTPException(status_code=404, detail="The requested protocol document section cannot be found")

@app.get("/v1/protocolDocumentVersions/{uuid}/sectionDefinition/{section_uuid}", 
  summary="Get the protocol document version section",
  description="Get the protococl document section for a study.",
  response_model=dict)
async def get_section_definition(uuid: str, section_uuid: str):
  doc = StudyProtocolDocumentVersion.find(uuid)
  if not 'error' in doc:
    result = doc.section_definition(section_uuid)
    return result
  else:
    raise HTTPException(status_code=404, detail="The requested protocol document section cannot be found")

@app.get("/v1/protocolDocumentVersions/{uuid}/section/{section_uuid}", 
  summary="Get the protocol document version section",
  description="Get the protococl document section for a study.",
  response_model=dict)
async def get_section(uuid: str, section_uuid: str):
  doc = StudyProtocolDocumentVersion.find(uuid)
  if doc:
    result = doc.section_read(section_uuid)
    print(f"SECTION: {result}")
    return {'text': result}
  else:
    raise HTTPException(status_code=404, detail="The requested protocol document section cannot be found")

class TextBody(BaseModel):
    text: str

@app.post("/v1/protocolDocumentVersions/{uuid}/section/{section_uuid}", 
  summary="Set the protocol document version section",
  description="Set the protococl document section for a study.",
  status_code=201,
  response_model=dict)
async def post_section(uuid: str, section_uuid: str, item: TextBody):
  doc = StudyProtocolDocumentVersion.find(uuid)
  if not 'error' in doc:
    result = doc.section_write(section_uuid, item.text)
    return result
  else:
    raise HTTPException(status_code=404, detail="The requested protocol document version cannot be found")

@app.get("/v1/protocolDocumentVersions/{uuid}/element/{name}", 
  summary="Get the protocol document version element",
  description="Get the protococl document element for a study.",
  response_model=dict)
async def get_element(uuid: str, name: str):
  doc = StudyProtocolDocumentVersion.find(uuid)
  if not 'error' in doc:
    #doc.set_study_version()
    definition = doc.element(name)
    result = doc.element_read(name)
    #print(f"RESULT: {name}={result}")
    if not 'error' in result:
      return {'uuid': uuid, 'definition': definition, 'data': result['result']}
    else:
      raise HTTPException(status_code=500, detail=result['error'])
  else:
    raise HTTPException(status_code=404, detail="The requested protocol document version cannot be found")

@app.post("/v1/protocolDocumentVersions/{uuid}/element/{name}", 
  summary="Set the protocol document version element",
  description="Set the protococl document element for a study.",
  status_code=201,
  response_model=dict)
async def write_element(uuid: str, name: str, item: TextBody):
  doc = StudyProtocolDocumentVersion.find(uuid)
  if not 'error' in doc:
    definition = doc.element(name)
    result = doc.element_write(name, item.text)
    #print(f"ELEMENT: {name}={result}")
    if not 'error' in result:
      return {'uuid': uuid, 'definition': definition, 'data': result['result']}
    else:
      raise HTTPException(status_code=500, detail=result['error'])
  else:
    raise HTTPException(status_code=404, detail=doc['error'])

@app.get("/v1/protocolDocumentVersions/{uuid}/document", 
  summary="Get a view of the protocol document version",
  description="Get the document view of the whole document or a section",
  response_model=str)
async def get_document_or_section(uuid: str, section: str = None):
  doc = StudyProtocolDocumentVersion.find(uuid)
  if doc:
    try:
      #doc.set_study_version()
      if section: 
        return doc.section_as_html(section)
      else:
        return doc.document_as_html()
    except Exception as e:
      raise HTTPException(status_code=500, detail=str(e))
  else:
    raise HTTPException(status_code=404, detail="The requested protocol document version cannot be found")

# Study Designs
# =============

@app.get("/v1/studyVersions/{uuid}/studyDesigns", 
  summary="Get the study designs for a study version",
  description="Provides the basic data for the study designs for a study version (currently limited to one design only).",
  response_model=dict)
async def list_study_designs(request: Request, page: int = 0, size: int = 0, filter: str=""):
  uuid = request.path_params['uuid']
  return StudyDesign.list(uuid, page, size, filter)

@app.get("/v1/studyVersions/{uuid}/studyDesigns_with_source", 
  summary="Get the study designs for a study version",
  description="Provides the basic data for the study designs for a study version (currently limited to one design only).",
  response_model=dict)
async def list_study_designs(request: Request, page: int = 0, size: int = 0, filter: str=""):
  uuid = request.path_params['uuid']
  results = {}
  results = StudyDesign.list_with_source(uuid, page, size, filter)
  results['page'] = page
  results['size'] = size
  results['filter'] = filter
  return results

@app.get("/v1/studyDesigns/{uuid}", 
  summary="Get the study design",
  description="Provides the details for a given study design.",
  response_model=StudyDesign)
async def get_study_design(uuid: str):
  study_design = StudyDesign.find(uuid)
  if study_design:
    return study_design
  else:
    raise HTTPException(status_code=404, detail="The requested study design cannot be found")

# JOHANNES
@app.post("/v1/studyDesigns", 
  summary="Create a new study design",
  description="Creates a study design. If succesful the uuid of the created resource is returned.",
  status_code=201,
  response_model=str)
async def create_study_design(name: str, background_tasks: BackgroundTasks, description: str="", label: str=""):
  result = StudyDesign.create(name, description, label)
  print("result", result)
  if not 'error' in result:
    return result
  else:
    raise HTTPException(status_code=409, detail=result['error'])

@app.get("/v1/studyDesigns/{uuid}/design", 
  summary="Get the study design design (arms and epochs)",
  description="Provides the high level design for a given study design.",
  response_model=dict)
async def get_study_design_summary(uuid: str):
  study_design = StudyDesign.find(uuid)
  if study_design:
    return study_design.design()
  else:
    raise HTTPException(status_code=404, detail="The requested study design cannot be found")

@app.get("/v1/studyDesigns/{uuid}/population", 
  summary="Get the study design population information",
  description="Provides the details population information for a given study design.",
  response_model=dict)
async def get_study_design_population(uuid: str):
  study_design = StudyDesign.find(uuid)
  if study_design:
    try:
      summary = study_design.summary()
      #print(f"Summary: {summary}")
      population = StudyDesignPopulation.find(summary['population']['uuid'])
      #print(f"Population: {population}")
      result = population.summary()
      #print(f"RESULT: {result}")
      for index, cohort in enumerate(result['cohorts']):
        x = StudyCohort.find(cohort['uuid'])
        result['cohorts'][index] = x.summary()
      return result
    except Exception as e:
      message = "Something went wrong building the study population information"
      application_logger.exception(message, e)
      raise HTTPException(status_code=500, detail=message)
  else:
    raise HTTPException(status_code=404, detail="The requested study design cannot be found")

# @app.delete("/v1/studies/{uuid}", 
#   summary="Delete a study",
#   description="Deletes the specified study.",
#   status_code=204)
# async def delete_study(uuid: str):
#   result = Study.delete(uuid)

# @app.get("/v1/studies/{uuid}", 
#   summary="Get a study",
#   description="Provides the detail for a specified study.",
#   response_model=Study)
# async def get_study(uuid: str):
#   study = Study.find_full(uuid)
#   if study == None:
#     raise HTTPException(status_code=404, detail="The requested study cannot be found")
#   else:
#     return study

# @app.get("/v1/studies/{uuid}/studyDesigns", 
#   summary="Get the study designs for a study",
#   description="Provides a list of uuids for te study designs that exisit for a specified study.",
#   response_model=List[StudyDesign])
# async def get_study_designs(uuid: str):
#   study = Study.find(uuid)
#   if study == None:
#     raise HTTPException(status_code=404, detail="The requested study cannot be found")
#   else:
#     return study.study_designs()

# @app.get("/v1/studies/{uuid}/parameters", 
#   summary="Get the study parameters (type, phase) for a study",
#   description="Provides a dictionary of the study parameters that exisit for a specified study.",
#   response_model=StudyParameters)
# async def get_study_parameters(uuid: str):
#   study = Study.find(uuid)
#   if study == None:
#     raise HTTPException(status_code=404, detail="The requested study cannot be found")
#   else:
#     return study.study_parameters()

# @app.get("/v1/studies/{uuid}/identifiers", 
#   summary="Get the study identifiers for a study",
#   description="Provides a dictionary of the study identifiers that exisit for a specified study.",
#   response_model=List[StudyIdentifier])
# async def get_study_identifiers(uuid: str):
#   study = Study.find(uuid)
#   if study == None:
#     raise HTTPException(status_code=404, detail="The requested study cannot be found")
#   else:
#     return study.study_identifiers()

# @app.post("/v1/studies/{uuid}/studyIdentifiers/sponsor", 
#   summary="Set a sponsor identifier for a study",
#   description="Sets a sponsor identifier for a study.",
#   status_code=201,
#   response_model=str)
# async def create_sponsor_identifiers(uuid: str, params: StudyIdentifierIn):
#   study = Study.find(uuid)
#   if study == None:
#     raise HTTPException(status_code=404, detail="The requested study cannot be found")
#   else:
#     return study.add_sponsor_identifier(params.identifier, params.name, params.scheme, params.scheme_identifier)

# @app.post("/v1/studies/{uuid}/studyIdentifiers/ctdotgov", 
#   summary="Set a CT.gov identifier for a study",
#   description="Sets a CT.gov identifier for a study. Only the NCT identifier needs to be provided.",
#   status_code=201,
#   response_model=str)
# async def create_sponsor_identifiers(uuid: str, params: StudyIdentifierIn):
#   study = Study.find(uuid)
#   if study == None:
#     raise HTTPException(status_code=404, detail="The requested study cannot be found")
#   else:
#     return study.add_ct_dot_gov_identifier(params.identifier)

@app.get("/v1/studyDesigns/{uuid}/dataContract", 
  summary="Get the data contract for a study design",
  description="Provides the data contract for a given study design.",
  response_model=dict)
async def get_study_design_data_contract(uuid: str, page: int=0, size: int=0, filter: str=""):
  study_design = StudyDesign.find(uuid)
  if study_design == None:
    raise HTTPException(status_code=404, detail="The requested study design cannot be found")
  else:
    return study_design.data_contract(page, size, filter)

@app.get("/v1/studyDesigns/{uuid}/sdtmDomains", 
  summary="Get the SDTM domains for a study design",
  description="Provides the SDTM domains for a given study design.",
  response_model=dict)
async def get_study_sdtm_domains(uuid: str, page: int=0, size: int=0, filter: str=""):
  study_design = StudyDesign.find(uuid)
  if study_design:
    return study_design.sdtm_domains(page, size, filter)
  else:
    raise HTTPException(status_code=404, detail="The requested study design cannot be found")

@app.get("/v1/studyDesigns/{uuid}/define", 
  summary="Get the define-xml for a study design",
  description="Provides the define-xml for a given study design.",
  response_model=dict)
async def get_study_sdtm_define(uuid: str, page: int=0, size: int=0, filter: str=""):
  study_design = StudyDesign.find(uuid)
  if study_design:
    return study_design.sdtm_define(page, size, filter)
  else:
    raise HTTPException(status_code=404, detail="The requested study design cannot be found")

@app.get("/v1/studyDesigns/{uuid}/forms",
  summary="Get the BC collection forms for a study design",
  description="Provides the forms from BCs for a given study design.",
  response_model=dict)
async def get_study_bc_forms(uuid: str, page: int=0, size: int=0, filter: str=""):
  study_design = StudyDesign.find(uuid)
  if study_design:
    return study_design.study_form(page, size, filter)
  else:
    raise HTTPException(status_code=404, detail="The requested study design cannot be found")

@app.get("/v1/studyDesigns/{uuid}/lab_transfer_spec",
  summary="Get a draft lab transfer spec for a study design",
  description="Provides the BCs that have their source = lab for a given study design.",
  response_model=dict)
async def get_study_lab_transfer(uuid: str, page: int=0, size: int=0, filter: str=""):
  study_design = StudyDesign.find(uuid)
  if study_design:
    return study_design.lab_transfer_spec(page, size, filter)
  else:
    raise HTTPException(status_code=404, detail="The requested study design cannot be found")

# @app.get("/v1/{uuid}/datapoint",
@app.get("/v1/datapoint_form",
  summary="Get the BC collection forms for a study design",
  description="Provides the forms from BCs for a given study design.",
  response_model=dict)
async def datapoint_form(datapoint: str, page: int=0, size: int=0, filter: str=""):
  if datapoint:
    return StudyDesign.datapoint_form(datapoint, page, size, filter)
  else:
    raise HTTPException(status_code=404, detail="No datapoint provided with api call")

@app.get("/v1/studyDesigns/{uuid}/subjectData", 
  summary="Get the subject data for a study design",
  description="Provides the subject data for a given study design.",
  response_model=dict)
async def get_study_design_soa(uuid: str, page: int=0, size: int=0, filter: str=""):
  study_design = StudyDesign.find(uuid)
  if study_design == None:
    raise HTTPException(status_code=404, detail="The requested study design cannot be found")
  else:
    return study_design.subject_data(page, size, filter)

@app.get("/v1/studyDesigns/{uuid}/subjects", 
  summary="Get the subjects for a study design",
  description="Provides the subjecta for a given study design.",
  response_model=dict)
async def get_study_design_subjects(uuid: str, page: int=0, size: int=0, filter: str=""):
  study_design = StudyDesign.find(uuid)
  if study_design == None:
    raise HTTPException(status_code=404, detail="The requested study design cannot be found")
  else:
    return study_design.subjects(page, size, filter)

@app.post('/v1/studyDesigns/{uuid}/dataFiles', 
  summary="Load study design data",
  description="Upload and process a CSV file loading the data into the database", 
  status_code=status.HTTP_201_CREATED,
  response_model=str)
async def create_study_data_file(request: Request, background_tasks: BackgroundTasks):
  form = await request.form()
  filename = form['upload_file'].filename
  data_type = form['dataType']
  contents = await form['upload_file'].read()
  df = DataFile()
  success = df.create(filename, contents, data_type)
  if success:
    background_tasks.add_task(df.execute)
    return df.uuid
  else:
    raise HTTPException(status_code=409, detail=f"Failed to upload the file. {df.error}")

@app.get('/update_ct', 
  summary="Add missing content for CT",
  description="Adds CT content not currently included when loading USDM protocol, e.g. pref_label",
  response_model=dict)
async def get_study_data_file_status(background_tasks: BackgroundTasks):
  response = DataFile.add_properties_to_ct()
  print("response", response)
  if response:
    return {'status': 'Done ct stuff'}
  else:
    raise HTTPException(status_code=404, detail="Couldn't add properties to CT")


@app.get("/v1/studyDesigns/{uuid}/dataFiles/{file_uuid}/status", 
  summary="Get study design data file status",
  description="Get the status of the processing for a given study desing data file",
  response_model=dict)
async def get_study_data_file_status(uuid: str, file_uuid: str):
  df = DataFile.find(file_uuid)
  if df:
    return df.get_status()
  else:
    raise HTTPException(status_code=404, detail="The requested study design data file cannot be found")

@app.get("/v1/studyDesigns/{uuid}/biomedicalConcepts/unlinked", 
  summary="Returns set of BCs not linked to domains",
  description="Returns the set of BCs within a study design that are not linked with SDTM domains.")
async def get_unlinked_bcs(uuid: str, page: int=0, size: int=0, filter: str=""):
  item = StudyDesign.find(uuid)
  if item:
    return item.biomedical_concepts_unlinked(page, size, filter)
  else:
    raise HTTPException(status_code=404, detail="The requested study design cannot be found")

@app.delete("/v1/studyDesigns/{uuid}", 
  summary="Delete a study design",
  description="Deletes the specified study design.",
  status_code=204)
async def delete_study_design(uuid: str):
  return StudyDesign.delete(uuid)


# Populations & Cohorts
# =====================
@app.get("/v1/studyDesignPopulations/{uuid}", 
  summary="Get the population details",
  description="Gets the details of the population",
  response_model=dict)
async def get_population_summary(request: Request, uuid: str):
  item = StudyDesignPopulation.find(uuid)
  if item:
    return item.summary()
  else:
    raise HTTPException(status_code=404, detail="The requested study design population cannot be found")

@app.get("/v1/studyCohorts/{uuid}/", 
  summary="Get the cohort details",
  description="Gets a details for a cohort",
  response_model=dict)
async def get_cohort_summary(request: Request, uuid: str):
  item = StudyCohort.find(uuid)
  if item:
    return item.summary()
  else:
    raise HTTPException(status_code=404, detail="The requested study design population cannot be found")

# Timelines
# =========

# JOHANNES JOBBAR
@app.get("/v1/studyDesigns/{uuid}/timelines", 
  summary="Get the timelines for a study design",
  description="Gets a list of timeliens for a study design.",
  response_model=dict)
async def list_timelines(request: Request, page: int = 0, size: int = 0, filter: str=""):
  uuid = request.path_params['uuid']
  return ScheduleTimeline.list(uuid, page, size, filter)

@app.get("/v1/studyDesigns/{uuid}/timelines_soa", 
  summary="Get the timelines and epochs for a study design",
  description="Gets a list of timelines and epochs for a study design.",
  response_model=dict)
async def list_soa_timelines(request: Request, page: int = 0, size: int = 0, filter: str=""):
  uuid = request.path_params['uuid']
  arms = StudyArm.list(uuid, page, size, filter)
  print("arms", uuid, arms)
  arm_timepoints = StudyEpoch.list_with_timepoints(uuid)
  timelines = ScheduleTimeline.list(uuid, page, size, filter)
  epochs = StudyEpoch.list(uuid, page, size, filter)
  stuff = StudyEpoch.list_with_elements(uuid)
  encounters = Encounter.list_with_timing(uuid)
  return {'timelines': timelines, 'epochs': epochs, 'arms': arms, 'encounters': encounters, 'arm_timepoints': arm_timepoints, 'stuff': stuff}

@app.post("/v1/timelines", 
  summary="Create a new timeline",
  description="Creates a timeline. If succesful the uuid of the created resource is returned.",
  status_code=201,
  response_model=str)
async def create_timeline(name: str, background_tasks: BackgroundTasks, description: str="", label: str="", template: str=""):
  result = ScheduleTimeline.create(name, description, label, template)
  print("result", result)
  if not 'error' in result:
    return result
  else:
    raise HTTPException(status_code=409, detail=result['error'])

@app.post("/v1/scheduledactivityinstances", 
  summary="Create a new scheduled activity instance",
  description="Creates a scheduled activity instance for a epoch. If succesful the uuid of the created resource is returned.",
  status_code=201,
  response_model=str)
async def create_scheduled_activity_instance(name: str, background_tasks: BackgroundTasks, description: str="", label: str=""):
  result = ScheduledActivityInstance.create(name, description, label)
  print("result", result)
  if not 'error' in result:
    return result
  else:
    raise HTTPException(status_code=409, detail=result['error'])


@app.get("/v1/timelines/{uuid}", 
  summary="Get a timeline",
  description="Provides the details for a given timeline.",
  response_model=list)
async def get_timeline_soa(uuid: str):
  timeline = ScheduleTimeline.find(uuid)
  if timeline:
    return timeline
  else:
    raise HTTPException(status_code=404, detail="The requested timeline cannot be found")

@app.get("/v1/timelines/{uuid}/soa", 
  summary="Get the SoA for a timeline",
  description="Provides the Schedule of Activities for a given timeline.",
  response_model=list)
async def get_timeline_soa(uuid: str):
  timeline = ScheduleTimeline.find(uuid)
  if timeline:
    return timeline.soa()
  else:
    raise HTTPException(status_code=404, detail="The requested study design cannot be found")

# # Arms
# # ====

# @app.get("/v1/studyDesigns/{uuid}/studyArms", 
#   summary="Get the arms for a study design",
#   description="Provides a list of uuids for the arms that exisit for a specified study.",
#   response_model=List[StudyArm])
# async def get_study_design_epochs(uuid: str):
#   study_design = StudyDesign.find(uuid)
#   if study_design == None:
#     raise HTTPException(status_code=404, detail="The requested study design cannot be found")
#   else:
#     return study_design.arms()

# Johannes
@app.post("/v1/studyDesigns/{uuid}/studyArms", 
  summary="Create a new arm within a study design.",
  description="Creates an arm.",
  status_code=201,
  response_model=str)
async def create_arm(uuid: str, name: str, background_tasks: BackgroundTasks, description: str="", label: str=""):
  result = StudyArm.create(name, description, label)
  if result == None:
    raise HTTPException(status_code=409, detail="Trying to create a duplicate arm within the study")
  else:
    return result

# # Epochs
# # ======

@app.post("/v1/studyEpochs", 
  summary="Create a new epoch",
  description="Creates a epoch. If succesful the uuid of the created resource is returned.",
  status_code=201,
  response_model=str)
async def create_epoch(name: str, background_tasks: BackgroundTasks, description: str="", label: str=""):
  result = StudyEpoch.create(name, description, label)
  print("result", result)
  if not 'error' in result:
    return result
  else:
    raise HTTPException(status_code=409, detail=result['error'])

# # Elements
# # ======

@app.post("/v1/studyElements", 
  summary="Create a new element",
  description="Creates a element. If succesful the uuid of the created resource is returned.",
  status_code=201,
  response_model=str)
async def create_element(name: str, background_tasks: BackgroundTasks, description: str="", label: str=""):
  result = StudyElement.create(name, description, label)
  print("result", result)
  if not 'error' in result:
    return result
  else:
    raise HTTPException(status_code=409, detail=result['error'])

# # Cells
# # ======

@app.post("/v1/studyCells", 
  summary="Create a new cell",
  description="Creates a cell. If succesful the uuid of the created resource is returned.",
  status_code=201,
  response_model=str)
async def create_cell(background_tasks: BackgroundTasks):
  result = StudyCell.create()
  print("result", result)
  if not 'error' in result:
    return result
  else:
    raise HTTPException(status_code=409, detail=result['error'])



# @app.get("/v1/studyDesigns/{uuid}/studyEpochs", 
#   summary="Get the epochs for a study design.",
#   description="Provides a list of uuids for the epochs that exisit for a specified study.",
#   response_model=List[StudyEpoch])
# async def get_study_design_epochs(uuid: str):
#   study_design = StudyDesign.find(uuid)
#   if study_design == None:
#     raise HTTPException(status_code=404, detail="The requested study design cannot be found")
#   else:
#     return study_design.epochs()

# @app.post("/v1/studyDesigns/{uuid}/studyEpochs", 
#   summary="Create a new epoch within a study design.",
#   description="Creates an epoch. The epoch is added to the end of the list of epochs for the specified study.",
#   status_code=201,
#   response_model=str)
# async def create_epoch(uuid: str, epoch: StudyEpochIn):
#   result = StudyEpoch.create(uuid, epoch.name, epoch.description)
#   if result == None:
#     raise HTTPException(status_code=409, detail="Trying to create a duplicate epoch within the study")
#   else:
#     return result

# JOHANNES
# @app.put("/v1/studyEpochs/{uuid}", 
#   summary="Update an epoch",
#   description="Update the simple fields of an epoch.",
#   status_code=201,
#   response_model=StudyEpoch)
# async def update_epoch(uuid: str, data: StudyEpochIn):
#   epoch = StudyEpoch.find(uuid)
#   if epoch == None:
#     raise HTTPException(status_code=404, detail="The requested epoch cannot be found")
#   else:
#     return epoch.update(data.name, data.description)

# Encounters
# ==========

@app.get("/v1/studyDesigns/{uuid}/encounters",
  summary="Get the encounters for a study design",
  description="Provides a list of uuids for the encounters that exisit for a specified study.",
  response_model=List[Encounter])
async def get_study_design_encounters(uuid: str, page: int = 0, size: int = 0, filter: str=""):
  study_design = StudyDesign.find(uuid)
  print("study_design", study_design)
  print("tjena")
  if study_design is None:
    raise HTTPException(status_code=404, detail="The requested study design cannot be found")
  else:
    return study_design.study_design_encounters(page=page, size=size, filter=filter)

# @app.post("/v1/studyDesigns/{uuid}/encounters", 
#   summary="Creates a new encounter within a study design",
#   description="Creates an encounter withn a study design.",
#   status_code=201,
#   response_model=str)
# async def create_encounter(uuid: str, encounter: EncounterIn):
#   result = Encounter.create(uuid, encounter.name, encounter.description)
#   if result == None:
#     raise HTTPException(status_code=409, detail="Trying to create a duplicate encounter within the study")
#   else:
#     return result

# @app.put("/v1/studyEpochs/{uuid}/encounters", 
#   summary="Links an encounter with an epoch",
#   description="Creates an link between an epoch and an encounter.",
#   status_code=201,
#   response_model=str)
# async def link_epoch_and_encounter(uuid: str, encounter: EncounterLink):
#   epoch = StudyEpoch.find(uuid)
#   if epoch == None:
#     raise HTTPException(status_code=404, detail="The requested epoch cannot be found")
#   else:
#     return epoch.add_encounter(encounter.uuid)

# @app.get("/v1/encounters/{uuid}", 
#   summary="Returns an encounter",
#   description="Returns the details about an encounter.",
#   response_model=Encounter)
# async def find_activity(uuid: str):
#   activity = Encounter.find(uuid)
#   if activity == None:
#     raise HTTPException(status_code=404, detail="The requested encounter cannot be found")
#   else:
#     return activity

# Activities
# ==========

# @app.post("/v1/studyDesigns/{uuid}/activities", 
#   summary="Create a new activity within a study",
#   description="Creates an activity. The activity is added to the end of the list of activities for the specified study.",
#   status_code=201,
#   response_model=str)
# async def create_activity(uuid: str, activity: ActivityIn):
#   result = Activity.create(uuid, activity.name, activity.description)
#   if result == None:
#     raise HTTPException(status_code=409, detail="Trying to create a duplicate activity within the study")
#   else:
#     return result

@app.get("/v1/activities/{uuid}", 
  summary="Returns an activity",
  description="Returns the details about an activity.",
  response_model=Activity)
async def find_activity(uuid: str):
  activity = Activity.find(uuid)
  if activity:
    return activity
  else:
    raise HTTPException(status_code=404, detail="The requested activity cannot be found")

# @app.post("/v1/activities/{uuid}/studyData", 
#   summary="Creates a new study data item within an activity",
#   description="Creates an an item of study data.",
#   status_code=201,
#   response_model=str)
# async def create_study_data(uuid: str, study_data: StudyDataIn):
#   activity = Activity.find(uuid)
#   result = activity.add_study_data(study_data.name, study_data.description, study_data.link)
#   return result

@app.get("/v1/activities/{uuid}/children", 
  summary="Returns the children of an activity",
  description="Returns the set of children related to the specified activity.",
  response_model=list)
async def find_activity_study_data(uuid: str):
  activity = Activity.find(uuid)
  if activity:
    result = activity.children()
    #print(f"RESULT: {result}")
    return result
  else:
    raise HTTPException(status_code=404, detail="The requested activity cannot be found")

# Domains
# =======

@app.get("/v1/domains/{uuid}", 
  summary="Returns an SDTM domain",
  description="Returns the SDTM.")
async def find_domain(uuid: str):
  item = Domain.find(uuid)
  #print(f"ITEM: {item}")
  if item:
    return item
  else:
    raise HTTPException(status_code=404, detail="The requested domain cannot be found")

@app.get("/v1/domains/{uuid}/data", 
  summary="Returns an SDTM domain",
  description="Returns the SDTM.")
async def find_domain(uuid: str):
  item = Domain.find(uuid)
  if item:
    return item.data()
  else:
    raise HTTPException(status_code=404, detail="The requested domain cannot be found")

@app.get("/v1/trial_design_domain/{uuid}/data", 
  summary="Returns an SDTM Trial Design domain",
  description="Returns the SDTM Trial Design domain.")
async def sdtm_trials_design_domain(uuid: str, domain_name: str, page: int = 0, size: int = 0, filter: str=""):
  study_design = StudyDesign.find(uuid)
  if study_design == None:
    raise HTTPException(status_code=404, detail="The requested study design cannot be found")
  else:
    return study_design.sdtm_trial_design_domain(domain_name, page, size, filter)

@app.get("/v1/domains/{uuid}/biomedicalConcepts/linked", 
  summary="Returns BCs for a domain",
  description="Returns the Biomedical Concepts linked to a specific SDTM domain.")
async def domain_bcs(uuid: str, page: int = 0, size: int = 0, filter: str=""):
  item = Domain.find(uuid)
  if item:
    return item.bcs(page, size, filter)
  else:
    raise HTTPException(status_code=404, detail="The requested domain cannot be found")

@app.get("/v1/domains/{uuid}/biomedicalConcepts/unlink", 
  summary="Unlink BC by name",
  description="Unlinks BCs from a fiven SDTM domian by name")
async def domain_unlink_bcs(uuid: str, name: str):
  item = Domain.find(uuid)
  if item:
    return item.unlink(name)
  else:
    raise HTTPException(status_code=404, detail="The requested domain cannot be found")

@app.get("/v1/domains/{uuid}/biomedicalConcepts/link", 
  summary="Link BC by name",
  description="Links BC by name to a specified SDTM domian")
async def domain_link_bcs(uuid: str, name: str):
  item = Domain.find(uuid)
  if item:
    return item.link(name)
  else:
    raise HTTPException(status_code=404, detail="The requested domain cannot be found")

# Biomedical Concepts
# ===================

# @app.get("/v1/studyDesigns/{uuid}/biomedicalConcepts/unlinked", 
#   summary="Returns an SDTM domain",
#   description="Returns the SDTM.")
# async def find_domain(uuid: str):
#   item = StudyDesign.find(uuid)
#   if item:
#     return item.biomedical_concepts_unlinked()
#   else:
#     raise HTTPException(status_code=404, detail="The requested study design cannot be found")
