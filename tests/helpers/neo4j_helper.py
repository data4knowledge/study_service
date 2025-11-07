from d4kms_generic import ServiceEnvironment
from neo4j import GraphDatabase

class Neo4jHelper():
  
  def __init__(self):
    self.__db_name = ServiceEnvironment().get('NEO4J_DB_NAME')
    self.__uri = ServiceEnvironment().get('NEO4J_URI')
    self.__usr = ServiceEnvironment().get('NEO4J_USERNAME')
    self.__pwd = ServiceEnvironment().get('NEO4J_PASSWORD')
    self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__usr, self.__pwd))

  def clear(self):
    with self.__driver.session(database=self.__db_name) as session:
      query = """
        CALL apoc.periodic.iterate('MATCH (n) RETURN n', 'DETACH DELETE n', {batchSize:1000})
      """
      session.run(query)
      session.close()

  def run(self, query):
    with self.__driver.session(database=self.__db_name) as session:
      session.run(query)
      session.close()

  def count(self):
    with self.__driver.session(database=self.__db_name) as session:
      query = """
        MATCH (n) RETURN COUNT(n) as count
      """
      result = session.run(query)
      record = result.single()
      session.close()
      return record['count']

  def close(self):
    self.__driver.close()