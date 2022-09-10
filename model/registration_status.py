from datetime import datetime
from pydantic import BaseModel
from model.registration_authority import RegistrationAuthority

class RegistrationStatus(BaseModel):
  registration_status = str
  effective_date = datetime
  until_date = datetime
  managed_by = RegistrationAuthority
