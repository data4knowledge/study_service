from test_data.service import Service

class StudyService(Service):

  def __init__(self, url):
    super().__init__(url)

  def send_study_file(self, full_path):
    url = "%sv1/studyFiles" % (self.url)
    file = {'upload_file': open(full_path, 'rb')}
    resp = self._file_post(url=url, files=file) 
    return resp

  def study_file_status(self, uuid):
    url = "%sv1/studyFiles/%s/status" % (self.url, uuid)
    return self._get(url)

  def clean(self):
    url = "%sv1/clean" % (self.url)
    return self._delete(url)

  def studies(self, page=1, size=10, filter=""):
    url = "%sv1/studies?page=%s&size=%s&filter=%s" % (self.url, page, size, filter)
    return self._get(url)

  # def _post(self, url, data={}):
  #   response = requests.post(url, json=data)       
  #   if response.status_code == 201:
  #     return json.loads(response.text)
  #   else:
  #     return { 'error': "Service failed to respond, error [%s][%s]" % (response.text, response.status_code)}

  # def _delete(self, url, data={}):
  #   response = requests.delete(url)       
  #   if response.status_code == 204:
  #     return {}
  #   else:
  #     return { 'error': "Service failed to respond, error [%s][%s]" % (response.text, response.status_code)}
