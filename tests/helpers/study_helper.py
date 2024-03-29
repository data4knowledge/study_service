from tests.helpers.neo4j_helper import Neo4jHelper
from tests.helpers.study_design_helper import StudyDesignHelper
from uuid import uuid4

class StudyHelper():
  
  def __init__(self, db, title):
    self.db = db
    self.uuid = str(uuid4())
    query = """
      CREATE (n:Study {uuid: '%s', studyTitle: '%s'}) RETURN n
    """ % (self.uuid, title)
    result = self.db.run(query)

  def add_study_design(self, study_design):
    query = """
      MATCH (n:Study {uuid: '%s'}), (m:StudyDesign {uuid: '%s'}) CREATE (n)-[:STUDY_DESIGN]->(m) RETURN n
    """ % (self.uuid, study_design.uuid)
    result = self.db.run(query)
    return None

  def add_phase(self, code):
    query = """
      MATCH (n:Study {uuid: '%s'}), (m:Code {uuid: '%s'}) CREATE (n)-[:STUDY_PHASE]->(m) RETURN n
    """ % (self.uuid, code.uuid)
    result = self.db.run(query)
    return None

  def add_type(self, code):
    query = """
      MATCH (n:Study {uuid: '%s'}), (m:Code {uuid: '%s'}) CREATE (n)-[:STUDY_TYPE]->(m) RETURN n
    """ % (self.uuid, code.uuid)
    result = self.db.run(query)
    return None