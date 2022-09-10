from fastapi import FastAPI, HTTPException
from model.system import SystemOut
from utility.service_environment import ServiceEnvironment
from pydantic import UUID4

tags_metadata = [
    {
       "name": "System Info",
       "description": "Provides information of the version of the BC micro service version."
        },
    {
        "name": "BC List",
        "description": "The listing of all BCs in the data base.",
    },
    {
        "name": "Specific Instance of a BC",
        "description": "Returns the BC that in the name of the BC contains the string specified in the <b>name</b> parameter."
        }
    
]

VERSION = "0.1"
SYSTEM_NAME = "d4k Study Build Microservice"

app = FastAPI(
  title = SYSTEM_NAME,
  description = "A microservice to handle Biomedical Concepts in a Neo4j database.",
  version = VERSION
 # ,openapi_tags=tags_metadata
)

@app.get("/", 
  summary="Get system and version",
  description="Returns the microservice system details and the version running.", 
  response_model=SystemOut)
async def read_root():
  return SystemOut(**{ 'system_name': SYSTEM_NAME, 'version': VERSION, 'environment': ServiceEnvironment().environment() })

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
