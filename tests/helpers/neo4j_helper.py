from utility.service_environment import ServiceEnvironment
from neo4j import GraphDatabase

class Neo4jHelper():
  
  def __init__(self):
    self.__db_name = ServiceEnvironment().get('NEO4J_DB_NAME')
    self.__uri = ServiceEnvironment().get('NEO4J_URL')
    self.__usr = ServiceEnvironment().get('NEO4J_USER')
    self.__pwd = ServiceEnvironment().get('NEO4J_PWD')
    self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__usr, self.__pwd))

  def clear(self):
    with self.__driver.session(database=self.__db_name) as session:
      query = """
        CALL apoc.periodic.iterate('MATCH (n) RETURN n', 'DETACH DELETE n', {batchSize:1000})
      """
      session.run(query)
      session.close()

  def close(self):
    self.__driver.close()