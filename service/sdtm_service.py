from .service import Service
from d4kms_generic import ServiceEnvironment

class SDTMService(Service):

  def __init__(self):
    url = ServiceEnvironment().get("SDTM_SERVER_URL")
    super().__init__(url, 1)
  
  async def managed_items(self, page=1, size=10, filter=""):
    url = "%sv1/managedItems" % (self.url)
    return await self._get(url)

  async def implementation_guides(self, page=1, size=10, filter=""):
    url = "%sv1/implementationGuides?page=%s&size=%s&filter=%s" % (self.url, page, size, filter)
    return await self._get(url)

  async def domains(self, page=1, size=10, filter=""):
    url = "%sv1/domains?page=%s&size=%s&filter=%s" % (self.url, page, size, filter)
    return await self._get(url)

  async def domain(self, uuid, expand=True):
    url = "%sv1/domains/%s?expand=%s" % (self.url, uuid, str(expand).lower())
    return await self._get(url)

  async def domain_bcs(self, uuid):
    url = "%sv1/domains/%s/biomedicalConcepts" % (self.url, uuid)
    return await self._get(url)

  async def variables(self, uuid, page=1, size=10, filter=""):
    url = "%sv1/domains/%s/variables?page=%s&size=%s&filter=%s" % (self.url, uuid, page, size, filter)
    return await self._get(url)
