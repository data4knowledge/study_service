from deta import Deta
from service_environment import ServiceEnvironment

class StoreHelper():
    
  def __init__(self):
    self.__deta = Deta(ServiceEnvironment().get('RA_SERVICE_PROJ_KEY'))
    self.__store = self.__deta.Base(ServiceEnvironment().get('RA_SERVICE_STORE'))
    
  def clear(self):
    items = self.__store.fetch()
    for item in items.items:
      self.__store.delete(item["key"])
