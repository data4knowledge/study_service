from uuid import uuid4

class CodeHelper():
  
  def __init__(self, db, code, code_system, version, decode):
    self.db = db
    self.uuid = str(uuid4())
    query = """
      CREATE (n:Code { uuid: '%s', code: '%s', codeSystem: '%s', codeSystemVersion: '%s', decode: '%s' }) RETURN n
    """ % (self.uuid, code, code_system, version, decode)
    result = self.db.run(query)