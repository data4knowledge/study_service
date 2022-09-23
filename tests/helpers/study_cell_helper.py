from tests.helpers.neo4j_helper import Neo4jHelper
from uuid import uuid4

class StudyCellHelper():
  
  def __init__(self, db):
    self.db = db
    self.uuid = str(uuid4())
    query = """
      CREATE (n:StudyCell {uuid: '%s' }) RETURN n
    """ % (self.uuid)
    result = self.db.run(query)

  def add_arm(self, arm):
    query = """
      MATCH (n:StudyCell {uuid: '%s' }), (m:StudyArm {uuid: '%s' })  
      CREATE (n)-[:STUDY_ARM]->(m) 
    """ % (self.uuid, arm.uuid)
    result = self.db.run(query)

  def add_epoch(self, epoch):
    query = """
      MATCH (n:StudyCell {uuid: '%s' }), (m:StudyEpoch {uuid: '%s' })  
      CREATE (n)-[:STUDY_EPOCH]->(m) 
    """ % (self.uuid, epoch.uuid)
    result = self.db.run(query)
