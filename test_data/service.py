import requests
import json

class Service():

  def __init__(self, url):
    self.url = url

  def status(self):
    return self._get(self.url)

  def _get(self, url):
    response = requests.get(url)
    if response.status_code == 200:
      return json.loads(response.text)
    else:
      return { 'error': "Service failed to respond. Error: %s, status: %s" % (response.text, response.status_code)}

  def _file_post(self, url, files, data={}):
    response = requests.post(url, files=files, data=data) if data else requests.post(url, files=files)
    if response.status_code == 201:
      return json.loads(response.text)
    else:
      return { 'error': "Service failed to respond, error [%s][%s]" % (response.text, response.status_code)}

  def _post(self, url, data={}):
    response = requests.post(url, json=data)       
    if response.status_code == 201:
      return json.loads(response.text)
    else:
      return { 'error': "Service failed to respond, error [%s][%s]" % (response.text, response.status_code)}

  def _delete(self, url, data={}):
    response = requests.delete(url)       
    if response.status_code == 204:
      return {}
    else:
      return { 'error': "Service failed to respond, error [%s][%s]" % (response.text, response.status_code)}
  