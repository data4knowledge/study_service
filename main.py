from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, status, Request, BackgroundTasks
from model.system import SystemOut
from model.study_file import StudyFile
from model.study import Study
from model.study_version import StudyVersion
from model.study_design import StudyDesign
from d4kms_service import Neo4jConnection
from model.schedule_timeline import ScheduleTimeline
from model.activity import Activity
from model.study_protocol_document_version import StudyProtocolDocumentVersion, SPDVBackground
# from model.study_identifier import StudyIdentifier, StudyIdentifierIn
# from model.study_design import StudyDesign
# from model.study_domain_instance import StudyDomainInstance
# from model.activity import Activity, ActivityIn
# from model.study_epoch import StudyEpoch, StudyEpochIn
# from model.study_arm import StudyArm, StudyArmIn
# from model.study_data import StudyData, StudyDataIn
# from model.encounter import Encounter, EncounterIn, EncounterLink
# from typing import List
from model.template.template_manager import template_manager
from d4kms_generic import ServiceEnvironment
from d4kms_generic import application_logger

VERSION = "0.8"
SYSTEM_NAME = "d4k Study Microservice"

app = FastAPI(
  title = SYSTEM_NAME,
  description = "A microservice to handle Study Builds in a Neo4j database.",
  version = VERSION
)

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

@app.delete("/v1/clean", 
  summary="Delete database",
  description="Deletes the entire database.",
  status_code=204)
async def delete_clean():
  db = Neo4jConnection()
  db.clear()

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
  if not success:
    raise HTTPException(status_code=409, detail=f"Failed to upload the file. {sf.error}")
  else:
    background_tasks.add_task(sf.execute)
    return sf.uuid

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
  if not 'error' in doc:
    result = doc.section_read(section_uuid)
    #print(f"SECTION: {result}")
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
async def get_section(uuid: str, section_uuid: str, item: TextBody):
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
    print(f"RESULT: {name}={result}")
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
    print(f"ELEMENT: {name}={result}")
    if not 'error' in result:
      return {'uuid': uuid, 'definition': definition, 'data': result['result']}
    else:
      raise HTTPException(status_code=500, detail=result['error'])
  else:
    raise HTTPException(status_code=404, detail=doc['error'])

# @app.get("/v1/protocolDocumentVersions/{uuid}/document", 
#   summary="Get a view of the protocol document version",
#   description="Get the document view of the whole document or a section",
#   response_model=str)
# async def get_document_or_section(uuid: str, section: str = None):
#   doc = StudyProtocolDocumentVersion.find(uuid)
#   if doc:
#     try:
#       #doc.set_study_version()
#       if section: 
#         return doc.section_as_html(section)
#       else:
#         return doc.document_as_html()
#     except Exception as e:
#       raise HTTPException(status_code=500, detail=str(e))
#   else:
#     raise HTTPException(status_code=404, detail="The requested protocol document version cannot be found")

# Study Designs
# =============

@app.get("/v1/studyVersions/{uuid}/studyDesigns", 
  summary="Get the study designs for a study version",
  description="Provides the basic data for the study designs for a study version (currently limited to one design only).",
  response_model=dict)
async def list_study_designs(request: Request, page: int = 0, size: int = 0, filter: str=""):
  uuid = request.path_params['uuid']
  return StudyDesign.list(uuid, page, size, filter)

@app.get("/v1/studyDesigns/{uuid}", 
  summary="Get the study design",
  description="Provides the details for a given study design.",
  response_model=StudyDesign)
async def get_study_design_soa(uuid: str):
  study_design = StudyDesign.find(uuid)
  if study_design:
    return study_design
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



# @app.get("/v1/studyDesigns/{uuid}/dataContract", 
#   summary="Get the data contract for a study design",
#   description="Provides the data contract for a given study design.",
#   response_model=dict)
# async def get_study_design_data_contract(uuid: str, page: int=0, size: int=0, filter: str=""):
#   study_design = StudyDesign.find(uuid)
#   if study_design == None:
#     raise HTTPException(status_code=404, detail="The requested study design cannot be found")
#   else:
#     return study_design.data_contract(page, size, filter)

# @app.get("/v1/studyDesigns/{uuid}/sdtmDomains", 
#   summary="Get the SDTM domains for a study design",
#   description="Provides the SDTM domains for a given study design.",
#   response_model=dict)
# async def get_study_sdtm_domains(uuid: str, page: int=0, size: int=0, filter: str=""):
#   study_design = StudyDesign.find(uuid)
#   if study_design == None:
#     raise HTTPException(status_code=404, detail="The requested study design cannot be found")
#   else:
#     return study_design.sdtm_domains(page, size, filter)

# @app.get("/v1/studyDesigns/{uuid}/subjectData", 
#   summary="Get the subject data for a study design",
#   description="Provides the subject data for a given study design.",
#   response_model=dict)
# async def get_study_design_soa(uuid: str, page: int=0, size: int=0, filter: str=""):
#   study_design = StudyDesign.find(uuid)
#   if study_design == None:
#     raise HTTPException(status_code=404, detail="The requested study design cannot be found")
#   else:
#     return study_design.subject_data(page, size, filter)

# Timelines
# =========

@app.get("/v1/studyDesigns/{uuid}/timelines", 
  summary="Get the timelines for a study design",
  description="Gets a list of timeliens for a study design.",
  response_model=dict)
async def list_timelines(request: Request, page: int = 0, size: int = 0, filter: str=""):
  uuid = request.path_params['uuid']
  return ScheduleTimeline.list(uuid, page, size, filter)

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

# @app.post("/v1/studyDesigns/{uuid}/studyArms", 
#   summary="Create a new arm within a study design.",
#   description="Creates an arm.",
#   status_code=201,
#   response_model=str)
# async def create_arm(uuid: str, arm: StudyArmIn):
#   result = StudyArm.create(uuid, arm.name, arm.description)
#   if result == None:
#     raise HTTPException(status_code=409, detail="Trying to create a duplicate arm within the study")
#   else:
#     return result

# # Epochs
# # ======

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

# # Encounters
# # ==========

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
    print(f"RESULT: {result}")
    return result
  else:
    raise HTTPException(status_code=404, detail="The requested activity cannot be found")

# # Domains
# # =======

# @app.get("/v1/domains/{uuid}", 
#   summary="Returns an SDTM domain",
#   description="Returns the SDTM.")
# async def find_domain(uuid: str):
#   item = StudyDomainInstance.find(uuid)
#   if item == None:
#     raise HTTPException(status_code=404, detail="The requested domain cannot be found")
#   else:
#     return item.data()
