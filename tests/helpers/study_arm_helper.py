from uuid import uuid4

class StudyArmHelper():
  
  def __init__(self, db, name, description):
    self.db = db
    self.uuid = str(uuid4())
    query = """
      CREATE (n:StudyArm {uuid: '%s', studyArmName: '%s', studyArmDesc: '%s'}) RETURN n
    """ % (self.uuid, name, description)
    result = self.db.run(query)
