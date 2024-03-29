from .service import Service
from d4kms_generic import ServiceEnvironment

class BCService(Service):

  def __init__(self):
    url = ServiceEnvironment().get("BC_SERVER_URL")
    super().__init__(url)

  def biomedical_concepts(self, type, page=1, size=10, filter=""):
    base_urls = {'d4k': 'v1/biomedicalConcepts', 'generic': 'v1/cdisc/biomedicalConcepts/generic', 'sdtm': 'v1/cdisc/biomedicalConcepts/sdtm'}
    base_url = base_urls[type]
    url = "%s?page=%s&size=%s&filter=%s" % (base_url, page, size, filter)
    print(f"URL: {url}")
    return self._get(url)

  def biomedical_concept(self, type, uuid):
    base_urls = {'d4k': 'v1/biomedicalConcepts', 'generic': 'v1/cdisc/biomedicalConcepts/generic', 'sdtm': 'v1/cdisc/biomedicalConcepts/sdtm'}
    base_url = base_urls[type]
    url = "%s/%s" % (base_url, uuid)
    print(f"URL: {url}")
    return self._get(url)
 