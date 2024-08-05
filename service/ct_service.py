import requests
from d4kms_generic import ServiceEnvironment

class CTService():

  def __init__(self):
    self.__api_url = ServiceEnvironment().get("CT_SERVER_URL")

  def api_get(self, url):
    headers =  {"Content-Type":"application/json"}
    response = requests.get("%s%s" % (self.__api_url, url), headers=headers)
    #print(response)
    return response.json()

  def find_notation(self, term, page=1, size=10, filter=""):
    path = f"v1/terms"
    url = f"{path}?notation={term}&page={page}&size={size}&filter={filter}"
    # print(f"URL: {url}")
    return self.api_get(url)

  def find_by_identifier(self, identifier, page=1, size=10, filter=""):
    path = f"v1/terms"
    url = f"{path}?identifier={identifier}&page={page}&size={size}&filter={filter}"
    # print(f"URL: {url}")
    return self.api_get(url)

if __name__ == "__main__":
  ct = CTService()

  # response = ct.index()
  # response = ct.codelist_index()
  response = ct.find_notation("SYSBP")
  print("respons[0]['parent'].keys()",response[0]['parent'].keys())
  # sdtm_cl = [cli for cli in response if 'SDTM' in cli['parent']['pref_label']]
  first_sdtm_term = next((cli for cli in response if 'SDTM' in cli['parent']['pref_label']),[])
  print(f"term found {first_sdtm_term['parent']['identifier']}.{first_sdtm_term['child']['identifier']}")

  response = ct.find_by_identifier(first_sdtm_term['child']['identifier'])
  first_sdtm_label = next((cli for cli in response if 'Test Name' in cli['parent']['pref_label']),[])
  print("first_sdtm_label",first_sdtm_label['child']['pref_label'])

  print("klart")
