import requests
from d4kms_generic import ServiceEnvironment

class CRMService():

  def __init__(self):
    self.__api_url = ServiceEnvironment().get("CRM_SERVER_URL")
  
  def crm_node_data_types(self, name):
    return self.api_get("v1/nodes/dataTypes?name=%s" % (name))

  def leaf(self, node, data_type, property):
    return self.api_get("v1/nodes/leaves?node=%s&dataType=%s&property=%s" % (node, data_type, property))

  def api_get(self, url):
    headers =  {"Content-Type":"application/json"}
    response = requests.get("%s%s" % (self.__api_url, url), headers=headers)
    #print(response)
    return response.json()