import pytest
from fastapi.testclient import TestClient
from main import app, VERSION as app_version
from uuid import uuid4
import os
from d4kms_service import Neo4jConnection
from model.data_file import DataFile

client = TestClient(app)

def reset_ct(study_design_uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[:BIOMEDICAL_CONCEPTS_REL]->(bc:BiomedicalConcept)-[]->(bcp:BiomedicalConceptProperty)-[]->(rc:ResponseCode)-[]->(c:Code)
        WHERE not c.updated is null
        RETURN DISTINCT c.code as code
      """ % (study_design_uuid)
      print(f"query: {query}")
      query_results = session.run(query)
      codes = []
      for result in query_results.data():
        codes.append(result['code'])
      if codes == []:
        return None

      query = """
        MATCH (c:Code {code: '%s'})
        REMOVE c.updated
        RETURN count(c) as count
      """ % (codes[0])
      print(f"query: {query}")
      query_results = session.run(query)
      results = [x for x in query_results.data()]
      return results

def test_multiple_ct():
    study_design_uuid = "bbe21af5-13f1-40f5-a8ba-45a513c28345"
    # code = reset_ct(study_design_uuid)
    # print("Resetted code", code)
    DataFile.add_properties_to_ct(study_design_uuid)

