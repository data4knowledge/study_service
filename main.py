from fastapi import FastAPI, HTTPException
from model.system import SystemOut
from model.study import Study, StudyIn
from model.activity import Activity, ActivityIn
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
  print("A")
  result = Activity.create(uuid, activity.name, activity.description)
  if result == None:
    raise HTTPException(status_code=409, detail="Trying to create a duplicate study")
  else:
    return result

# @app.get("/v1/studies",
#   summary="BC Listing",
#   description="The listing of all BCs in the data base.", 
#   response_model=StudyList)
# async def get_biomedical_concepts(page: int = 0, size: int = 0, filter: str=""):
#   return StudyList.list(page, size, filter)

# @app.get("/v1/studies/{uuid}",
#   summary="BC Instance",
#   description="Specific Instance of a BC.", 
#   response_model=Study)
# async def get_bc(uuid: UUID4):
#   result = Study.find(str(uuid))
#   if result == None:
#     raise HTTPException(status_code=404, detail="item not found")
#   else:
#     return result
