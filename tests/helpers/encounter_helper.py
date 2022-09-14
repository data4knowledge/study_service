from uuid import uuid4

class EncounterHelper():
  
  def __init__(self, db, name, description):
    self.db = db
    self.uuid = str(uuid4())
    query = """
      CREATE (n:Encounter {uuid: '%s', encounterName: '%s', encounterDesc: '%s'}) RETURN n
    """ % (self.uuid, name, description)
    result = self.db.run(query)
