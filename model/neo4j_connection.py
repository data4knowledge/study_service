from neo4j import GraphDatabase
from utility.service_environment import ServiceEnvironment

class Neo4jConnection():
  
  def __init__(self):
    self.__db_name = ServiceEnvironment().get('NEO4J_DB_NAME')
    self.__url = ServiceEnvironment().get('NEO4J_URL')
    self.__usr = ServiceEnvironment().get('NEO4J_USER')
    self.__pwd = ServiceEnvironment().get('NEO4J_PWD')
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

  def query(self, query, db=None):
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