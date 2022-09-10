from neo4j_model import Base
from neo4j_model.neo4j_database import Neo4jDatabase
#from ra_server.ra_server import RaServer

class RegistrationAuthority(Base):

  uri = str
  name = str
  ra_uuid = str
  
  # @classmethod
  # def find(cls, uuid):
  #   db = Neo4jDatabase()
  #   ra = RegistrationAuthority.match(db.graph(), uuid).first()
  #   ra._check_complete(db)
  #   return ra

  # def _check_complete(self, db):
  #   if self.name == "" or self.name == None:
  #     server = RaServer()
  #     ra = server.registration_authority_by_uri(self.uri)
  #     self.name = ra['name']
  #     self.ra_uuid = ra['uuid']
  #     db.graph().push(self)