import pytest
from fastapi.testclient import TestClient
from main import app, VERSION as app_version
from uuid import uuid4
import os
from tests.helpers.neo4j_helper import Neo4jHelper
from tests.helpers.study_helper import StudyHelper
from tests.helpers.study_design_helper import StudyDesignHelper
from tests.helpers.activity_helper import ActivityHelper
from tests.helpers.study_epoch_helper import StudyEpochHelper
from tests.helpers.encounter_helper import EncounterHelper
from tests.helpers.workflow_helper import WorkflowHelper
from tests.helpers.code_helper import CodeHelper

client = TestClient(app)

def test_root_1():
  response = client.get("/")
  assert response.status_code == 200
  assert response.json() == { 'system_name': 'd4k Study Build Microservice', 'version': app_version, 'environment': 'test' }

def test_root_2():
  response = client.get("/v1")
  assert response.status_code == 200
  assert response.json() == { 'system_name': 'd4k Study Build Microservice', 'version': app_version, 'environment': 'test' }

def test_add_study_ok():
  store = Neo4jHelper()
  store.clear()
  body = {
    "title": "123",
    "identifier": "NZ123",
  }
  response = client.post("/v1/studies", json=body)
  assert response.status_code == 201
  store.close()
  
def test_add_study_exists():
  store = Neo4jHelper()
  store.clear()
  body = {
    "title": "123",
    "identifier": "NZ123",
  }
  response = client.post("/v1/studies", json=body)
  assert response.status_code == 201
  response = client.post("/v1/studies", json=body)
  assert response.status_code == 409
  store.close()

def test_study_list():
  store = Neo4jHelper()
  store.clear()
  response = client.get("/v1/studies")
  assert response.status_code == 200
  store.close()

def test_get_study_ok():
  store = Neo4jHelper()
  store.clear()
  body = {
    "title": "123",
    "identifier": "NZ123",
  }
  response = client.post("/v1/studies", json=body)
  response = client.get("/v1/studies/%s" % (response.json()))
  assert response.status_code == 200
  store.close()

def test_get_study_parameters_ok():
  db = Neo4jHelper()
  db.clear()
  study = StudyHelper(db, "A title")
  code = CodeHelper(db, "A", "system", "1", "Letter A")
  study.add_phase(code)
  code = CodeHelper(db, "B", "system", "1", "Letter B")
  study.add_type(code)
  response = client.get("/v1/studies/%s/parameters" % (study.uuid))
  assert response.status_code == 200
  db.close()

def test_study_design_list_ok():
  store = Neo4jHelper()
  store.clear()
  body = {
    "title": "123",
    "identifier": "NZ123",
  }
  response = client.post("/v1/studies", json=body)
  response = client.get("/v1/studies")
  study = response.json()['items'][0]['uuid']
  response = client.get("/v1/studies/%s/studyDesigns" % (study))
  assert response.status_code == 200
  store.close()

def test_study_design_epoch_list_ok():
  store = Neo4jHelper()
  store.clear()
  body = {
    "title": "123",
    "identifier": "NZ123",
  }
  response = client.post("/v1/studies", json=body)
  response = client.get("/v1/studies")
  study = response.json()['items'][0]['uuid']
  response = client.get("/v1/studies/%s/studyDesigns" % (study))
  study_design = response.json()[0]['uuid']
  response = client.get("/v1/studyDesigns/%s/studyEpochs" % (study_design))
  assert response.status_code == 200
  store.close()

def test_delete_study():
  store = Neo4jHelper()
  store.clear()
  assert store.count() == 0
  body = {
    "title": "123",
    "identifier": "NZ123",
  }
  response = client.post("/v1/studies", json=body)
  assert response.status_code == 201
  assert store.count() == 12
  response = client.delete("/v1/studies/%s" % (response.json()))
  assert response.status_code == 204
  assert store.count() == 0
  store.close()

def test_add_first_study_activity_ok():
  db = Neo4jHelper()
  db.clear()
  study = StudyHelper(db, "A title")
  study_design = StudyDesignHelper(db)
  study.add_study_design(study_design)
  # Activity
  body = {
    "name": "DM",
    "description": "Demographics",
  }
  response = client.post("/v1/studyDesigns/%s/activities" % (study_design.uuid), json=body)
  assert response.status_code == 201
  db.close()

def test_add_second_study_activity_ok():
  db = Neo4jHelper()
  db.clear()
  study = StudyHelper(db, "A title")
  study_design = StudyDesignHelper(db)
  study.add_study_design(study_design)
  # Activity
  body = {
    "name": "DM",
    "description": "Demographics",
  }
  response = client.post("/v1/studyDesigns/%s/activities" % (study_design.uuid), json=body)
  assert response.status_code == 201
  body = {
    "name": "DM2",
    "description": "Demographics 2",
  }
  response = client.post("/v1/studyDesigns/%s/activities" % (study_design.uuid), json=body)
  assert response.status_code == 201
  db.close()

def test_add_third_study_activity_ok():
  db = Neo4jHelper()
  db.clear()
  study = StudyHelper(db, "A title")
  study_design = StudyDesignHelper(db)
  study.add_study_design(study_design)
  # Activity
  body = {
    "name": "DM",
    "description": "Demographics",
  }
  response = client.post("/v1/studyDesigns/%s/activities" % (study_design.uuid), json=body)
  assert response.status_code == 201
  body = {
    "name": "DM2",
    "description": "Demographics 2",
  }
  response = client.post("/v1/studyDesigns/%s/activities" % (study_design.uuid), json=body)
  assert response.status_code == 201
  body = {
    "name": "DM3",
    "description": "Demographics 3",
  }
  response = client.post("/v1/studyDesigns/%s/activities" % (study_design.uuid), json=body)
  assert response.status_code == 201
  db.close()

def test_add_duplicate_study_activity_error():
  db = Neo4jHelper()
  db.clear()
  study = StudyHelper(db, "A title")
  study_design = StudyDesignHelper(db)
  study.add_study_design(study_design)
  # Activity
  body = {
    "name": "DM",
    "description": "Demographics",
  }
  response = client.post("/v1/studyDesigns/%s/activities" % (study_design.uuid), json=body)
  assert response.status_code == 201
  response = client.post("/v1/studyDesigns/%s/activities" % (study_design.uuid), json=body)
  assert response.status_code == 409
  db.close()

def test_add_study_data_ok():
  db = Neo4jHelper()
  db.clear()
  activity = ActivityHelper(db)
  # Activity
  body = {
    "name": "DM",
    "description": "Demographics",
    "link": "http://www.example.com/a"
  }
  response = client.post("/v1/activities/%s/studyData" % (activity.uuid), json=body)
  assert response.status_code == 201
  db.close()

def test_add_first_study_encounter_ok():
  db = Neo4jHelper()
  db.clear()
  study = StudyHelper(db, "A title")
  study_design = StudyDesignHelper(db)
  study.add_study_design(study_design)
  body = {
    "name": "V1",
    "description": "Visit 1",
  }
  response = client.post("/v1/studyDesigns/%s/encounters" % (study_design.uuid), json=body)
  assert response.status_code == 201
  db.close()

def test_add_second_study_encounter_ok():
  db = Neo4jHelper()
  db.clear()
  study = StudyHelper(db, "A title")
  study_design = StudyDesignHelper(db)
  study.add_study_design(study_design)
  body = {
    "name": "V1",
    "description": "Visit 1",
  }
  response = client.post("/v1/studyDesigns/%s/encounters" % (study_design.uuid), json=body)
  assert response.status_code == 201
  body = {
    "name": "V2",
    "description": "Visit 2",
  }
  response = client.post("/v1/studyDesigns/%s/encounters" % (study_design.uuid), json=body)
  assert response.status_code == 201
  db.close()

def test_add_third_study_encounter_ok():
  db = Neo4jHelper()
  db.clear()
  study = StudyHelper(db, "A title")
  study_design = StudyDesignHelper(db)
  study.add_study_design(study_design)
  body = {
    "name": "V1",
    "description": "Visit 1",
  }
  response = client.post("/v1/studyDesigns/%s/encounters" % (study_design.uuid), json=body)
  assert response.status_code == 201
  body = {
    "name": "V2",
    "description": "Visit 2",
  }
  response = client.post("/v1/studyDesigns/%s/encounters" % (study_design.uuid), json=body)
  assert response.status_code == 201
  body = {
    "name": "V3",
    "description": "Visit 3",
  }
  response = client.post("/v1/studyDesigns/%s/encounters" % (study_design.uuid), json=body)
  assert response.status_code == 201
  db.close()

def test_add_duplicate_encounter_error():
  db = Neo4jHelper()
  db.clear()
  study = StudyHelper(db, "A title")
  study_design = StudyDesignHelper(db)
  study.add_study_design(study_design)
  body = {
    "name": "V1",
    "description": "Visit 1",
  }
  response = client.post("/v1/studyDesigns/%s/encounters" % (study_design.uuid), json=body)
  assert response.status_code == 201
  response = client.post("/v1/studyDesigns/%s/encounters" % (study_design.uuid), json=body)
  assert response.status_code == 409
  db.close()

def test_link_epooch_encounter_ok():
  db = Neo4jHelper()
  db.clear()
  epoch = StudyEpochHelper(db, "Epoch", "Epoch Desc")
  encounter = EncounterHelper(db, "V1", "Visit 1")
  body = {
    "uuid": encounter.uuid
  }
  response = client.put("/v1/studyEpochs/%s/encounters" % (epoch.uuid), json=body)
  assert response.status_code == 201
  db.close()

def test_add_first_study_workflow_ok():
  db = Neo4jHelper()
  db.clear()
  study = StudyHelper(db, "A title")
  study_design = StudyDesignHelper(db)
  study.add_study_design(study_design)
  body = {
    "name": "SoA",
    "description": "The SoA workflow",
  }
  response = client.post("/v1/studyDesigns/%s/workflows" % (study_design.uuid), json=body)
  assert response.status_code == 201
  db.close()

def test_link_workflow_activity_encounter_ok():
  db = Neo4jHelper()
  db.clear()
  wf = WorkflowHelper(db, "Item", "Description")
  activity = ActivityHelper(db, "Act1", "A Description")
  encounter = EncounterHelper(db, "E1", "E Description")
  body = {
    "description": "Something",
    "activity_uuid": activity.uuid,
    "encounter_uuid": encounter.uuid,
  }
  response = client.post("/v1/workflows/%s/workflowItems" % (wf.uuid), json=body)
  assert response.status_code == 201
  db.close()

def test_add_epoch_ok():
  db = Neo4jHelper()
  db.clear()
  sd = StudyDesignHelper(db)
  body = {
    "name": "New Epoch",
    "description": "Something"
  }
  response = client.post("/v1/studyDesigns/%s/epochs" % (sd.uuid), json=body)
  assert response.status_code == 201
  db.close()
