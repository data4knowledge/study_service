from fastapi import FastAPI, HTTPException
from model.system import SystemOut
from model.study import Study, StudyIn
from model.activity import Activity, ActivityIn
from model.study_epoch import StudyEpoch
from model.study_data import StudyData, StudyDataIn
from model.encounter import Encounter, EncounterIn
from utility.service_environment import ServiceEnvironment
import uuid

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

@app.post("/v1/activities/{uuid}/studyData", 
  summary="Creates a new study data item within an activity",
  description="Creates an an item of study data.",
  status_code=201,
  response_model=str)
async def create_study_data(uuid: str, study_data: StudyDataIn):
  activity = Activity.find(uuid)
  result = activity.add_study_data(study_data.name, study_data.description, study_data.link)
  return result


