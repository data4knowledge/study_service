import requests
from d4kms_generic import application_logger

class Service():

  def __init__(self, url):
    self._url = url
    self._headers = {"Content-Type": "application/json"}

  def _get(self, url):
    try:
      response = requests.get(f"{self._url}{url}", headers=self._headers)
      if response.status_code == 200:
        return response.json()
      else:
        application_logger.error(f"Service request responded with error, error code {response.status_code}, response {response.text}")
        return {}
    except Exception as e:
      application_logger.exception("Exception raised in service", e)
      return {}
