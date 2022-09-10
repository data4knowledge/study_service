from pydantic import BaseModel

class SystemOut(BaseModel):
  system_name: str
  version: str
  environment: str