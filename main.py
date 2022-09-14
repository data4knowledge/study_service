from fastapi import FastAPI, HTTPException
from model.system import SystemOut
from model.study import Study, StudyIn, StudyList
from model.activity import Activity, ActivityIn
from model.study_epoch import StudyEpoch
from model.study_data import StudyData, StudyDataIn
from model.encounter import Encounter, EncounterIn, EncounterLink
from model.workflow import Workflow, WorkflowIn
from utility.service_environment import ServiceEnvironment

VERSION = "0.1"
SYSTEM_NAME = "d4k Study Build Microservice"

app = FastAPI(
  title = SYSTEM_NAME,
  description = "A microservice to handle Study Builds in a Neo4j database.",
  version = VERSION
 # ,openapi_tags=tags_metadata
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

@app.get("/v1/studies", 
  summary="List of studies",
  description="Provide a list of all studies.",
  status_code=200,
  response_model=StudyList)
async def list_studies():
  return Study.list()

@app.post("/v1/studies", 
  summary="Create a new study",
  description="Creates a study. If succesful the uuid of the created resource is returned.",
  status_code=201,
  response_model=str)
async def create_study(study: StudyIn):
  result = Study.create(study.identifier, study.title)
  if result == None:
    raise HTTPException(status_code=409, detail="Trying to create a duplicate study")
  else:
    return result

@app.delete("/v1/studies/{uuid}", 
  summary="Delete a study",
  description="Deletes the specified study.",
  status_code=204)
async def delete_study(uuid: str):
  result = Study.delete(uuid)

@app.get("/v1/studies/{uuid}/studyDesigns", 
  summary="Get the study designs for a study",
  description="Provides a list of uuids for te study designs that exisit for a specified study.",
  status_code=200)
async def get_study_designs(uuid: str):
  study = Study.find(uuid)
  if study == None:
    raise HTTPException(status_code=404, detail="The requested study cannot be found")
  else:
    return study.study_designs()

@app.post("/v1/studyDesigns/{uuid}/activities", 
  summary="Create a new activity within a study",
  description="Creates an activity. The activity is added to the end of the list of activities for the specified study.",
  status_code=201,
  response_model=str)
async def create_activity(uuid: str, activity: ActivityIn):
  result = Activity.create(uuid, activity.name, activity.description)
  if result == None:
    raise HTTPException(status_code=409, detail="Trying to create a duplicate activity within the study")
  else:
    return result

@app.post("/v1/studyDesigns/{uuid}/encounters", 
  summary="Creates a new encounter within a study design",
  description="Creates an encounter withn a study design.",
  status_code=201,
  response_model=str)
async def create_encounter(uuid: str, encounter: EncounterIn):
  result = Encounter.create(uuid, encounter.name, encounter.description)
  if result == None:
    raise HTTPException(status_code=409, detail="Trying to create a duplicate encounter within the study")
  else:
    return result

@app.post("/v1/studyDesigns/{uuid}/workflows", 
  summary="Creates a new workflow within a study design",
  description="Creates an workflow withn a study design.",
  status_code=201,
  response_model=str)
async def create_workflow(uuid: str, wf: WorkflowIn):
  result = Encounter.create(uuid, wf.name, wf.description)
  if result == None:
    raise HTTPException(status_code=409, detail="Trying to create a duplicate workflow within the study")
  else:
    return result

@app.post("/v1/activities/{uuid}/studyData", 
  summary="Creates a new study data item within an activity",
  description="Creates an an item of study data.",
  status_code=201,
  response_model=str)
async def create_study_data(uuid: str, study_data: StudyDataIn):
  activity = Activity.find(uuid)
  result = activity.add_study_data(study_data.name, study_data.description, study_data.link)
  return result

@app.put("/v1/studyEpochs/{uuid}/encounters", 
  summary="Links an encounter with an epoch",
  description="Creates an link between an epoch and an encounter.",
  status_code=201,
  response_model=str)
async def link_epoch_and_encounter(uuid: str, encounter: EncounterLink):
  epoch = StudyEpoch.find(uuid)
  result = epoch.add_encounter(encounter.uuid)
  return result

