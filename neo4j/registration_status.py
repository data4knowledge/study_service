from py2neo.ogm import Model, Property, RelatedTo
from neo4j.registration_authority import RegistrationAuthority

class RegistrationStatus(Model):
  registration_status = Property()
  effective_date = Property()
  until_date = Property()
  
  managed_by = RelatedTo(RegistrationAuthority, "MANAGED_BY")