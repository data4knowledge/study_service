from py2neo.ogm import Repository
from service_environment import ServiceEnvironment

class Neo4jDatabase():
  
  def __init__(self):
    db_name = ServiceEnvironment().get('NEO4J_DB_NAME')
    url = ServiceEnvironment().get('NEO4J_URL')
    usr = ServiceEnvironment().get('NEO4J_USER')
    pwd = ServiceEnvironment().get('NEO4J_PWD')
    self.__repo = Repository(url, name=db_name, user=usr, password=pwd)

  def repository(self):
    return self.__repo

  def graph(self):
    return self.__repo.graph