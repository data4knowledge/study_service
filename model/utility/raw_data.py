import os
import json
import csv
from pathlib import Path
from d4kms_service import Neo4jConnection
import pandas as pd

def output_csv(path, name, data):
    OUTPUT_FILE = path / name
    if OUTPUT_FILE.exists():
        os.unlink(OUTPUT_FILE)
    print("Saving to",OUTPUT_FILE)
    output_variables = list(data[0].keys())
    with open(OUTPUT_FILE, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=output_variables)
        writer.writeheader()
        writer.writerows(data)

def clear_created_nodes():
    db = Neo4jConnection()
    with db.session() as session:
        query = "match (n:Datapoint|DataPoint) detach delete n return count(n)"
        results = session.run(query)
        print("Removing Datapoint/DataPoint",results.data())
        query = "match (n:Subject) detach delete n return count(n)"
        results = session.run(query)
        print("Removing Subject",results.data())
    db.close()
    
def get_bc_properties():
    query = f"""
        MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst:ScheduledActivityInstance)-[:ENCOUNTER_REL]-(enc)
        return bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, enc.label as ENCOUNTER_LABEL, dc.uri as DC_URI
    """
    query = """
        MATCH(study:Study{name:'Study_CDISC PILOT - LZZT'})-[r1:VERSIONS_REL]->(StudyVersion)-[r2:STUDY_DESIGNS_REL]->(sd:StudyDesign)
        MATCH(sd)-[r3:SCHEDULE_TIMELINES_REL]->(tl:ScheduleTimeline) where not (tl)<-[:TIMELINE_REL]-()
        MATCH(tl)-[r4:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)-[r5:ACTIVITY_REL]->(act:Activity)-[r6:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)-[r7:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bcp)<-[:PROPERTIES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(act_inst_main)-[:ENCOUNTER_REL]-(enc)
        return distinct bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, enc.label as ENCOUNTER_LABEL, dc.uri as DC_URI
    """
    db = Neo4jConnection()
    with db.session() as session:
        results = session.run(query)
        res = [result.data() for result in results]
    db.close()
    return res

def get_bc_properties_sub_timeline():
    query = """
        match (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bcp)<-[:PROPERTIES_REL]-(ignore_dc:DataContract)
        match (ignore_dc)-[:INSTANCES_REL]->(enc_msai:ScheduledActivityInstance)-[:ENCOUNTER_REL]->(enc:Encounter)
        with distinct bc.name as BC_NAME, bc.label as BC_LABEL, bcp.name as bcp_name, bcp.label as bcp_label, enc.label as ENCOUNTER_LABEL, enc_msai.uuid as enc_msai_uuid
        MATCH (msai:ScheduledActivityInstance {uuid: enc_msai_uuid})<-[:INSTANCES_REL]-(dc:DataContract)-[:INSTANCES_REL]->(sub_sai:ScheduledActivityInstance)
        MATCH (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty {name: bcp_name})<-[:PROPERTIES_REL]-(bc:BiomedicalConcept {name: BC_NAME})
        MATCH (sub_sai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
        WITH BC_NAME, BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, sub_sai.name as sub_sai_name, ENCOUNTER_LABEL, t.value as TIMEPOINT_VALUE, dc.uri as DC_URI
        return BC_NAME, BC_LABEL, BCP_NAME, BCP_LABEL, ENCOUNTER_LABEL, TIMEPOINT_VALUE, DC_URI
    """
    db = Neo4jConnection()
    with db.session() as session:
        results = session.run(query)
        res = [result.data() for result in results]
    db.close()
    return res

def get_bc_properties_ae():
    query = """
        MATCH (study:Study{name:'Study_CDISC PILOT - LZZT'})-[r1:VERSIONS_REL]->(StudyVersion)-[r2:STUDY_DESIGNS_REL]->(sd:StudyDesign)
        WITH sd
        match (sd)-[:SCHEDULE_TIMELINES_REL]->(tl:ScheduleTimeline {name:'Adverse Event Timeline'})-[:INSTANCES_REL]->(sai:ScheduledActivityInstance)
        match (sai:ScheduledActivityInstance)<-[:INSTANCES_REL]-(dc:DataContract)
        match (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
        return bc.label as BC_LABEL, bcp.name as BCP_NAME, bcp.label as BCP_LABEL, dc.uri as DC_URI,"" as ENCOUNTER_LABEL
    """
    # print("ae query", query)
    db = Neo4jConnection()
    with db.session() as session:
        results = session.run(query)
        res = [result.data() for result in results]
    db.close()
    return res

def read_raw_data_file(file):
    df = pd.read_csv(file)
    df = df.fillna("")
    data = df.to_dict(orient="records")
    return data

def get_unique_activities(data):
    unique_activities = {}
    for item in data:
        if item['TIMEPOINT'] != "":
            activity = {'visit':item['VISIT'],'label':item['LABEL'],'variable':item['VARIABLE'],'timepoint':item['TIMEPOINT']}
            key = str(activity)
        elif item['VISIT'] != "":
            activity = {'visit':item['VISIT'],'label':item['LABEL'],'variable':item['VARIABLE']}
            key = str(activity)
        else:
            activity = {'label':item['LABEL'],'variable':item['VARIABLE']}
            key = str(activity)
        if not key in unique_activities:
            unique_activities[key] = activity
    return unique_activities

def create_enrolment_file(raw_data, OUTPUT_PATH):
    print("\ncreate enrolment file")
    subject_no = set([r['SUBJID'] for r in raw_data])

    # Make it look like the previous load file
    subjects = []
    for subjid in subject_no:
        item = {}
        item['STUDY_URI'] = "https://study.d4k.dk/study-cdisc-pilot-lzzt"
        item['SITEID'] = "701"
        item['SUBJID'] = subjid
        subjects.append(item)

    filename = "enrolment_msg.csv"
    output_csv(OUTPUT_PATH, filename, subjects)
    return filename

def create_datapoint_file(raw_data, OUTPUT_PATH):
    print("\ncreate datapoint file")
    properties = get_bc_properties()
    properties_ae = get_bc_properties_ae()

    # NOTE: Not all blood pressure measurements are repeated, so data contracts for SCREENING 1, SCREENING 2, BASELINEWEEK 2, WEEK 4, WEEK 6, WEEK 8
    # All records marked as baseline are STANDING VSREPNUM = 3 -> PT2M. So I'll use that for them
    properties_sub_timeline = get_bc_properties_sub_timeline()

    unique_activities = get_unique_activities(raw_data)
    missing = []
    datapoints = []
    for k,v in unique_activities.items():
        rows = []
        if 'timepoint' in v:
            dc = next((i['DC_URI'] for i in properties_sub_timeline if i['BC_LABEL'] == v['label'] and i['ENCOUNTER_LABEL'] == v['visit'] and i['BCP_NAME'] == v['variable'] and i['TIMEPOINT_VALUE'] == v['timepoint']),[])
            if dc:
                rows = [r for r in raw_data if r['LABEL'] == v['label'] and r['VISIT'] == v['visit'] and r['VARIABLE'] == v['variable'] and r['TIMEPOINT'] == v['timepoint']]
        elif 'visit' in v:
            dc = next((i['DC_URI'] for i in properties if i['BC_LABEL'] == v['label'] and i['ENCOUNTER_LABEL'] == v['visit'] and i['BCP_LABEL'] == v['variable']),[])
            # NOTE: There is a mismatches within the BC specializations, So sometimes the label matches, sometimes the name
            if not dc:
                dc = next((i['DC_URI'] for i in properties if i['BC_LABEL'] == v['label'] and i['ENCOUNTER_LABEL'] == v['visit'] and i['BCP_NAME'] == v['variable']),[])
            if dc:
                rows = [r for r in raw_data if r['LABEL'] == v['label'] and r['VISIT'] == v['visit'] and r['VARIABLE'] == v['variable']]
        else:
            # AE is the only thing that is not visit bound at the moment
            dc = next((i['DC_URI'] for i in properties_ae if i['BC_LABEL'] == v['label'] and i['BCP_NAME'] == v['variable']),[])
            if dc:
                rows = [r for r in raw_data if r['LABEL'] == v['label'] and r['VARIABLE'] == v['variable']]

        for row in rows:
            item = {}
            item['SUBJID'] = row['SUBJID']
            fixed_label = row['LABEL'].replace(" ","").replace("-","")
            fixed_variable = row['VARIABLE'].replace(" ","").replace("-","")
            thing = f"{fixed_label}/{fixed_variable}"
            if 'TIMEPOINT' in row and row['TIMEPOINT']:
                dp_uri = f"{dc}{row['SUBJID']}/{thing}/{row['ROW_NO']}/{row['TIMEPOINT']}"
                record_key = f"{fixed_label}/{row['SUBJID']}/{row['ROW_NO']}/{row['TIMEPOINT']}"
            elif 'VISIT' in row and row['VISIT']:
                dp_uri = f"{dc}{row['SUBJID']}/{thing}/{row['ROW_NO']}"
                record_key = f"{fixed_label}/{row['SUBJID']}/{row['ROW_NO']}"
            else:
                dp_uri = f"{dc}{row['SUBJID']}/{thing}/{row['ROW_NO']}"
                record_key = f"{fixed_label}/{row['SUBJID']}/{row['ROW_NO']}"
            item['DATAPOINT_URI'] = dp_uri
            item['VALUE'] = row['VALUE']
            item['DC_URI'] = dc
            item['RECORD_KEY'] = record_key
            datapoints.append(item)

        if not dc:
            missing.append(v)

    filename = "datapoints_msg.csv"
    output_csv(OUTPUT_PATH, filename, datapoints)
    return filename

def import_raw_data(path, filename):
    OUTPUT_PATH = Path.cwd() / path
    raw_data_file = Path.cwd() / path / filename
    raw_data = read_raw_data_file(raw_data_file)
    enrolment_file = create_enrolment_file(raw_data, OUTPUT_PATH)
    datapoint_file = create_datapoint_file(raw_data, OUTPUT_PATH)
    return {'identifiers':enrolment_file, 'datapoints': datapoint_file}
