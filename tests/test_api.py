import pytest
from fastapi.testclient import TestClient
from main import app, VERSION as app_version
from uuid import uuid4
import os
from tests.helpers.neo4j_helper import Neo4jHelper

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

def test_add_study_activity_ok():
  store = Neo4jHelper()
  store.clear()
  # Add study
  body = {
    "title": "123",
    "identifier": "NZ123",
  }
  response = client.post("/v1/studies", json=body)
  uuid = response.json()
  # Activity
  body = {
    "name": "DM",
    "description": "Demographics",
  }
  response = client.post("/v1/studies/%s/activities" % (uuid), json=body)
  assert response.status_code == 201
  store.close()