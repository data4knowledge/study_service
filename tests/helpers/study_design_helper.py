from tests.helpers.neo4j_helper import Neo4jHelper
from uuid import uuid4

class StudyDesignHelper():
  
  def __init__(self, db):
    self.db = db
    self.uuid = uuid4()
    query = """
      CREATE (n:StudyDesign {uuid: '%s' }) RETURN n
    """ % (self.uuid)
    result = self.db.run(query)
