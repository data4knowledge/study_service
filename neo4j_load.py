from neo4j import GraphDatabase
import os
import glob
from stringcase import pascalcase, snakecase
from utility.service_environment import ServiceEnvironment

files = [
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-activity-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-address-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-alias_code-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-biomedical_concept_property-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-biomedical_concept_surrogate-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-biomedical_concept-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-code-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-eligibility_criteria-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-encounter-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-endpoint-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-geographic_scope-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-governance_date-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-indication-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-narrative_content-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-objective-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-organization-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-procedure-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-response_code-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-schedule_timeline_exit-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-schedule_timeline-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-scheduled_activity_instance-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-study_amendment_reason-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-study_amendment-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-study_arm-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-study_cell-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-study_design_population-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-study_design-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-study_element-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-study_epoch-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-study_identifier-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-study_protocol_document_version-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-study_protocol_document-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-study_version-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-study-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-subject_enrollment-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-syntax_template_dictionary-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-timing-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/node-transition_rule-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-activities-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-activity_timeline-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-activity-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-amendments-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-bc_properties-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-bc_property_response_codes-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-bc_surrogate-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-bc_surrogates-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-biomedical_concept-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-biomedical_concepts-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-business_therapeutic_areas-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-category-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-code-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-codes-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-content_child-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-contents-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-country-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-data_origin_type-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-date_values-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-default_condition-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-defined_procedures-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-dictionaries-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-dictionary-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-document_version-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-documented_by-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-encounter_contact_modes-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-encounter_environmental_setting-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-encounters-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-enrollments-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-epoch-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-geographic_scopes-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-intervention_model-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-level-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-next_activity-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-next_encounter-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-next_study_epoch-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-objective_endpoints-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-organization_legal_address-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-planned_sex_of_participants-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-previous_activity-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-previous_encounter-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-previous_study_epoch-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-primary_reason-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-protocol_status-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-relative_from_scheduled_instance-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-relative_to_scheduled_instance-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-schedule_timeline_entry-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-schedule_timeline_exit-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-schedule_timeline_exits-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-schedule_timeline_instances-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-scheduled_activity_instance_encounter-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-scheduled_instance_timings-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-secondary_reasons-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-standard_code-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-study_arm-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-study_arms-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-study_cells-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-study_design_blinding_scheme-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-study_designs-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-study_element-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-study_elements-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-study_eligibility_critieria-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-study_epoch-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-study_epochs-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-study_identifier_scope-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-study_identifiers-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-study_indications-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-study_objectives-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-study_phase-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-study_populations-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-study_schedule_timelines-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-therapeutic_areas-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-timing_relative_to_from-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-transition_end_rule-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-transition_start_rule-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-trial_intent_types-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-trial_types-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-type-1.csv', 
  'ff1767a0-6cdd-4dc3-a8d0-43beef8de038/rel-versions-1.csv'
]

def file_load(driver, database, sv, files):
  project_root = sv.get("GITHUB_BASE")
  load_files = []
  for filename in files:
    parts = filename.split("-")
    file_path = os.path.join(project_root, filename)
    print(f"PATH: {file_path}")
    if parts[0] == "node":
      load_files.append({ "label": pascalcase(parts[1]), "filename": file_path })
    else:
      load_files.append({ "type": parts[1].upper(), "filename": file_path })
  result = None
  session = driver.session(database=database)
  nodes = []
  relationships = []
  for file_item in load_files:
    if "label" in file_item:
      nodes.append("{ fileName: '%s', labels: ['%s'] }" % (file_item["filename"], file_item["label"]) )
    else:
      relationships.append("{ fileName: '%s', type: '%s' }" % (file_item["filename"], file_item["type"]) )
  query = """CALL apoc.import.csv( [%s], [%s], {stringIds: false})""" % (", ".join(nodes), ", ".join(relationships))
  result = session.run(query)
  for record in result:
    print(record)
    return_value = {'nodes': record['nodes'], 'relationships': record['relationships'], 'time': record['time']}
  driver.close()
  return return_value

def clear(tx):
  tx.run("CALL apoc.periodic.iterate('MATCH (n) RETURN n', 'DETACH DELETE n', {batchSize:1000})")

def clear_neo4j(driver, database):
  with driver.session(database=database) as session:
    session.write_transaction(clear)
  driver.close()

sv = ServiceEnvironment()
db_name = sv.get('NEO4J_DB_NAME')
url = sv.get('NEO4J_URI')
usr = sv.get('NEO4J_USERNAME')
pwd = sv.get('NEO4J_PASSWORD')
driver = GraphDatabase.driver(url, auth=(usr, pwd))

print("Deleting database ...")
clear_neo4j(driver, db_name)
print("Database deleted. Load new data ...")
result = file_load(driver, db_name, sv, files)
print("Load complete. %s nodes and %s relationships loaded in %s milliseconds." % (result['nodes'], result['relationships'], result['time']))
