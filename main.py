from fastapi import FastAPI, HTTPException, status, Response, Request, BackgroundTasks
from uuid import UUID
from model.system import SystemOut
from model.study_file import StudyFile
from model.study import Study, StudyList
# from model.study_identifier import StudyIdentifier, StudyIdentifierIn
# from model.study_design import StudyDesign
# from model.study_domain_instance import StudyDomainInstance
# from model.activity import Activity, ActivityIn
# from model.study_epoch import StudyEpoch, StudyEpochIn
# from model.study_arm import StudyArm, StudyArmIn
# from model.study_data import StudyData, StudyDataIn
# from model.encounter import Encounter, EncounterIn, EncounterLink
# from model.workflow import Workflow, WorkflowIn
# from model.workflow_item import WorkflowItem, WorkflowItemIn
from utility.service_environment import ServiceEnvironment
# from typing import List
import logging
import traceback

VERSION = "0.5"
SYSTEM_NAME = "d4k Study Microservice"

#logging.basicConfig(level=logging.DEBUG)

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

# Study Files
# ===========

@app.post('/v1/studyFiles', 
  summary="Load a study",
  description="Upload and process a study Excel file loading the data into the database", 
  status_code=status.HTTP_201_CREATED,
  response_model=str)
async def create_format_file(request: Request, background_tasks: BackgroundTasks):
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

<<<<<<< Updated upstream
@app.get("/v1/studies", 
  summary="List of studies",
  description="Provide a list of all studies.",
  status_code=200,
  response_model=StudyList)
async def list_studies(page: int = 0, size: int = 0, filter: str=""):
  return StudyList.list(page, size, filter)
=======
@app.get("/v1/studyFiles/{uuid}", 
  summary="Get study file status",
  description="Get the status of the processing for a given study file",
  response_model=str)
async def get_study(uuid: str):
  sf = StudyFile.find(uuid)
  if sf:
    return sf.status
  else:
    raise HTTPException(status_code=404, detail="The requested study cannot be found")

  # @app.get("/v1/studies", 
#   summary="List of studies",
#   description="Provide a list of all studies.",
#   status_code=200,
#   response_model=StudyList)
# async def list_studies(page: int = 0, size: int = 0, filter: str=""):
#   return StudyList.list(page, size, filter)
>>>>>>> Stashed changes

# @app.post("/v1/studies", 
#   summary="Create a new study",
#   description="Creates a study. If succesful the uuid of the created resource is returned.",
#   status_code=201,
#   response_model=str)
# async def create_study(study: StudyIn):
#   result = Study.create(study.identifier, study.title)
#   if result == None:
#     raise HTTPException(status_code=409, detail="Trying to create a duplicate study")
#   else:
#     return result

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

# # Study Designs
# # =============

# @app.get("/v1/studyDesigns/{uuid}/workflows", 
#   summary="Get the workflows for a study design",
#   description="Provides a list of uuids for the workflows that exisit for a specified study.",
#   response_model=List[Workflow])
# async def get_study_design_epochs(uuid: str):
#   study_design = StudyDesign.find(uuid)
#   if study_design == None:
#     raise HTTPException(status_code=404, detail="The requested study design cannot be found")
#   else:
#     return study_design.workflows()

# @app.get("/v1/studyDesigns/{uuid}/soa", 
#   summary="Get the SoA for a study design",
#   description="Provides the Schedule of Activities for a given study design.",
#   response_model=list)
# async def get_study_design_soa(uuid: str):
#   study_design = StudyDesign.find(uuid)
#   if study_design == None:
#     raise HTTPException(status_code=404, detail="The requested study design cannot be found")
#   else:
#     return study_design.soa()

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

# @app.post("/v1/studyDesigns/{uuid}/workflows", 
#   summary="Creates a new workflow within a study design",
#   description="Creates an workflow withn a study design.",
#   status_code=201,
#   response_model=str)
# async def create_workflow(uuid: str, wf: WorkflowIn):
#   result = Encounter.create(uuid, wf.name, wf.description)
#   if result == None:
#     raise HTTPException(status_code=409, detail="Trying to create a duplicate workflow within the study")
#   else:
#     return result

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

# # Encounters
# # ==========

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

# # Activities
# # ==========

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

# @app.get("/v1/activities/{uuid}", 
#   summary="Returns an activity",
#   description="Returns the details about an activity.",
#   response_model=Activity)
# async def find_activity(uuid: str):
#   activity = Activity.find(uuid)
#   if activity == None:
#     raise HTTPException(status_code=404, detail="The requested activity cannot be found")
#   else:
#     return activity

# @app.post("/v1/activities/{uuid}/studyData", 
#   summary="Creates a new study data item within an activity",
#   description="Creates an an item of study data.",
#   status_code=201,
#   response_model=str)
# async def create_study_data(uuid: str, study_data: StudyDataIn):
#   activity = Activity.find(uuid)
#   result = activity.add_study_data(study_data.name, study_data.description, study_data.link)
#   return result

# @app.get("/v1/activities/{uuid}/studyData", 
#   summary="Returns the study data for an activity",
#   description="Returns the set of study data children related to the specified activity.",
#   response_model=List[StudyData])
# async def find_activity_study_data(uuid: str):
#   activity = Activity.find(uuid)
#   if activity == None:
#     raise HTTPException(status_code=404, detail="The requested activity cannot be found")
#   else:
#     return activity.study_data()

# # Workflow
# # ========

# @app.post("/v1/workflows/{uuid}/workflowItems", 
#   summary="Creates an encounter and an activity to a workflow",
#   description="Creates an encounter and an activity to a workflow",
#   status_code=201,
#   response_model=str)
# async def create_workflow_item(uuid: str, wfi: WorkflowItemIn):
#   workflow = Workflow.find(uuid)
#   result = workflow.add_workflow_item(wfi.description, wfi.encounter_uuid, wfi.activity_uuid)
#   return result

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
