from pydantic import BaseModel

class RegistrationStatus(BaseModel):
  registration_status = str
  effective_date = str
  until_date = str
  owner: str
