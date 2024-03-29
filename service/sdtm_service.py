from .service import Service
from d4kms_generic import ServiceEnvironment

class SDTMService(Service):

  def __init__(self):
    url = ServiceEnvironment().get("SDTM_SERVER_URL")
    super().__init__(url)
  
  def managed_items(self, page=1, size=10, filter=""):
    url = "v1/managedItems"
    return self._get(url)

  def implementation_guides(self, page=1, size=10, filter=""):
    url = "v1/implementationGuides?page=%s&size=%s&filter=%s" % (page, size, filter)
    return self._get(url)

  def domains(self, page=1, size=10, filter=""):
    url = "v1/domains?page=%s&size=%s&filter=%s" % (page, size, filter)
    return self._get(url)

  def domain(self, uuid, expand=True):
    url = "v1/domains/%s?expand=%s" % (uuid, str(expand).lower())
    return self._get(url)

  def domain_bcs(self, uuid):
    url = "v1/domains/%s/biomedicalConcepts" % (uuid)
    return self._get(url)

  def variables(self, uuid, page=1, size=10, filter=""):
    url = "v1/domains/%s/variables?page=%s&size=%s&filter=%s" % (uuid, page, size, filter)
    return self._get(url)
