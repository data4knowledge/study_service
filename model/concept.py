from .api_base_model import ApiBaseModel
from neo4j_model.scoped_identifier import ScopedIdentifier
from neo4j_model.registration_status import RegistrationStatus

class Concept(ApiBaseModel):
  uri = str
  label = str
  
  # identified_by = RelatedTo(ScopedIdentifier, "IDENTIFIED_BY")
  # has_status = RelatedTo(RegistrationStatus, "HAS_STATUS")
  # previous = RelatedTo('Concept', "PREVIOUS")
