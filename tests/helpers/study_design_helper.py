from tests.helpers.neo4j_helper import Neo4jHelper
from uuid import uuid4

class StudyDesignHelper():
  
  def __init__(self, db):
    self.db = db
    self.uuid = str(uuid4())
    query = """
      CREATE (n:StudyDesign {uuid: '%s' }) RETURN n
    """ % (self.uuid)
    result = self.db.run(query)

  def add_cell(self, cell):
    query = """
      MATCH (n:StudyDesign {uuid: '%s' }), (m:StudyCell {uuid: '%s' })  
      CREATE (n)-[:STUDY_CELL]->(m) 
    """ % (self.uuid, cell.uuid)
    result = self.db.run(query)