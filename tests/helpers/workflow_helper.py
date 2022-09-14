from tests.helpers.neo4j_helper import Neo4jHelper
from uuid import uuid4

class WorkflowHelper():
  
  def __init__(self, db, name= "", description=""):
    self.db = db
    self.uuid = str(uuid4())
    query = """
      CREATE (n:Workflow {uuid: '%s', workflowName: '%s', workflowDesc: '%s' }) RETURN n
    """ % (self.uuid, name, description)
    result = self.db.run(query)
