import httpx
import json

class Service():

  def __init__(self, url, retries=0):
    self.client = httpx.AsyncClient()
    self.url = url
    self.retries = retries

  async def status(self):
    return await self._get(self.url)

  async def _get(self, url, retries=0):
    try:
      response = await self.client.get(url)
      if response.status_code == 200:
        return json.loads(response.text)
      else:
        return { 'error': "Service failed to respond. Error: %s, status: %s" % (response.text, response.status_code)}
    except httpx.ReadTimeout:
      if retries <= self.retries:
        result = await self._get(url, retries + 1)
        return result
      else:
        return { 'error': "Service timed out"}
    except httpx.ConnectError:
      return { 'error': "Failed to connect to service"}

  def _file_post(self, url, files, data={}):
    #print(f"URL: {url}")
    response = httpx.post(url, files=files, data=data) if data else httpx.post(url, files=files)
    if response.status_code == 201:
      return json.loads(response.text)
    else:
      return { 'error': "Service failed to respond, error [%s][%s]" % (response.text, response.status_code)}

  def _post(self, url, data={}):
    response = httpx.post(url, json=data)       
    if response.status_code == 201:
      return json.loads(response.text)
    else:
      return { 'error': "Service failed to respond, error [%s][%s]" % (response.text, response.status_code)}

  def _delete(self, url, data={}):
    response = httpx.delete(url)       
    if response.status_code == 204:
      return {}
    else:
      return { 'error': "Service failed to respond, error [%s][%s]" % (response.text, response.status_code)}
