import argparse
import time

from model.utility.utility import read_yaml_file
from d4kms_service import Neo4jConnection
from test_data.model.study_service import StudyService
from test_data.model.dummy_study import DummyStudy

if __name__ == "__main__":

  parser = argparse.ArgumentParser(
    prog='Study Service Test Data Injections Utility',
    description='Will inject a set of data into the study service database',
    epilog='Note: Not that sophisticated! :)'
  )
  parser.add_argument('filename', help="The path to, including the name of, the YAML configuration file.") 
  parser.add_argument('url', help="The service base url.")
  args = parser.parse_args()
  filename = args.filename
  url = args.url

  service = StudyService(url)
  config = read_yaml_file(filename)

  db = Neo4jConnection()
  db.clear()

  for xl in config['full_studies']:
    waiting = True
    results = service.send_study_file(xl['excel'])
    while waiting:
      status = service.study_file_status(results)
      print(f"STATUS: {status}")
      time.sleep(1)
      if status['percentage'] >= 100:
        waiting = False

  dummy_study = DummyStudy()    
  for study in config['dummy_studies']:
    result = dummy_study.create(study)
    print(f"Dummy study result: {result}")

