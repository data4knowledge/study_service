import requests
from d4kms_generic import ServiceEnvironment
from urllib.parse import urlparse, urlunparse

class RAService():

  def __init__(self):
    self.__api_url = ServiceEnvironment().get("RA_SERVER_URL")
  
  def registration_authority_by_uri(self, uri):
    req_parts = urlparse(uri)
    server_parts = urlparse(self.__api_url)
    uri = urlunparse((server_parts[0], server_parts[1], req_parts[2], "", "", ""))
    return self.api_full_get(uri)

  def namespace_by_name(self, name):
    return self.api_get("v1/namespaces?name=%s" % (name))

  def registration_authority_by_namespace_uuid(self, uuid):
    return self.api_get("v1/registrationAuthorities?namespace=%s" % (uuid))
    
  def api_get(self, url):
    headers =  {"Content-Type":"application/json"}
    response = requests.get("%s%s" % (self.__api_url, url), headers=headers)
    return response.json()

  def api_full_get(self, url):
    headers =  {"Content-Type":"application/json"}
    response = requests.get(url, headers=headers)
    return response.json()