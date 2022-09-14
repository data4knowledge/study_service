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
  assert store.count() == 7
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

def test_add_duplicate_study_activity_ok():
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
