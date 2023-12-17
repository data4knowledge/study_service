from neo4j import GraphDatabase
from utility.service_environment import ServiceEnvironment

class Neo4jConnection():
  
  def __init__(self):
    sv = ServiceEnvironment()
    self.__db_name = sv.get('NEO4J_DB_NAME')
    self.__url = sv.get('NEO4J_URI')
    self.__usr = sv.get('NEO4J_USERNAME')
    self.__pwd = sv.get('NEO4J_PASSWORD')
    try:
      self.__driver = GraphDatabase.driver(self.__url, auth=(self.__usr, self.__pwd))
      self.__error = None
    except Exception as e:
      self.__driver = None
      self.__error = e

  def session(self):
    return self.__driver.session(database=self.__db_name)

  def close(self):
    if self.__driver is not None:
      self.__driver.close()

  def query(self, query):
    self.__error = None
    assert self.__driver is not None, "Driver not initialized!"
    session = None
    response = None
    try: 
      session = self.__driver.session(database=self.__db_name)
      response = list(session.run(query))
    except Exception as e:
      self.__error = e
    finally: 
      if session is not None:
        session.close()
    return response

  def error(self):
    return self.__error
  
  def clear(self):
    query = """
      CALL apoc.periodic.iterate('MATCH (n) RETURN n', 'DETACH DELETE n', {batchSize:1000})
    """
    self.query(query)

  def count(self):
    with self.__driver.session(database=self.__db_name) as session:
      result = session.run('MATCH (n) RETURN COUNT(n) as count')
      record = result.single()
      session.close()
      return record['count']