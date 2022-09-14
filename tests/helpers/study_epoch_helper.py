from uuid import uuid4

class StudyEpochHelper():
  
  def __init__(self, db, name, description):
    self.db = db
    self.uuid = uuid4()
    query = """
      CREATE (n:StudyEpoch {uuid: '%s', studyEpochName: '%s', studyEpochDesc: '%s'}) RETURN n
    """ % (self.uuid, name, description)
    result = self.db.run(query)
