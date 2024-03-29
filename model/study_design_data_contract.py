import re
from d4kms_service import Neo4jConnection

class StudyDesignDataContract():

  @classmethod
  def create(cls, name, uri_root):
    db = Neo4jConnection()
    with db.session() as session:
      session.execute_write(cls._set_data_contract, name, cls._parse_name(name), uri_root)

  @classmethod
  def read(cls, uuid, page, size, filter):
    skip_offset_clause = ""
    if page != 0:
      offset = (page - 1) * size
      skip_offset_clause = "SKIP %s LIMIT %s" % (offset, size)
    db = Neo4jConnection()
    with db.session() as session:
      # query = """MATCH (sd:StudyDesign {uuid: '%s'})-[]->(wf:Workflow)-[]->(wfi:WorkflowItem)-[]->(bc:StudyBCInstance)-[]->(i:StudyBCItem)-[]->(dt:StudyBCDataType) WHERE i.collect=True
      #   RETURN COUNT(dt) as count
      # """ % (uuid)
      query = """
        MATCH(sd:StudyDesign {uuid: '%s'})
        OPTIONAL MATCH(sd)-[:ACTIVITIES_REL]-(act:Activity)
        OPTIONAL MATCH(act)<-[:ACTIVITY_REL]-(act_inst:ScheduledActivityInstance)<-[:INSTANCES_REL]-(tl:ScheduleTimeline)
        OPTIONAL MATCH(act)-[:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)
        OPTIONAL MATCH(bc)-[:PROPERTIES_REL]->(bc_prop:BiomedicalConceptProperty)
        MATCH(dc:DataContract)-[:PROPERTIES_REL]->(bc_prop)
        MATCH(dc)-[:INSTANCES_REL]->(act_inst)
        OPTIONAL MATCH(act_inst)-[:ENCOUNTER_REL]->(e:Encounter)
        RETURN COUNT(dc) as count
        """ % (uuid)
      #print(f"QUERY 1: {query}")
      result = session.run(query)
      count = 0
      for record in result:
        count = record['count']
      # query = """MATCH (sd:StudyDesign {uuid: '%s'})-[]->(wf:Workflow)-[]->(wfi:WorkflowItem)
      #   WITH wfi
      #   MATCH (a:Activity)<-[]-(wfi)-[]->(v:Encounter), (wfi)-[]->(bc:StudyBCInstance)-[]->(i:StudyBCItem)-[]->(dt:StudyBCDataType)-[:HAS_STUDY_BC_DATA_TYPE_PROPERTY *1..]->(dtp:StudyBCDataTypeProperty) WHERE i.collect=True AND dtp.collect=true
      #   WITH a.activityName as activity, v.encounterName as visit, bc.name as bc_name, bc.reference_uri as bc_uri, i.name as item, dt.name as data_type, dtp.name as property, dtp.uri as data_uri 
      #   RETURN DISTINCT activity, visit, bc_name, bc_uri, data_uri, item, data_type, property ORDER BY visit, activity, bc_name, item %s""" % (uuid, skip_offset_clause)
      query = """
        call apoc.cypher.run("MATCH(sd:StudyDesign{uuid:$s})-[r3:SCHEDULE_TIMELINES_REL]->(tl:ScheduleTimeline) where not (tl)<-[:TIMELINE_REL]-()
        MATCH(tl)-[r4:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)-[r5:ACTIVITY_REL]->(act:Activity)-[r6:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)-[r7:PROPERTIES_REL]->(bc_prop:BiomedicalConceptProperty)<-[r8:PROPERTIES_REL]-(dc:DataContract)-[r9:INSTANCES_REL]->(act_inst_main)
        MATCH(tl)-[r11:TIMINGS_REL]->(tpt:Timing)-[r12:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]->(act_inst_main)-[r13:ENCOUNTER_REL]->(e:Encounter)
        return distinct sd, 
        tl, 
        e,
        tpt,
        act, 
        act_inst_main,
        null as act_inst,  
        bc,
        bc_prop,
        dc
              UNION
        MATCH(sd:StudyDesign{uuid:$s})-[r3:SCHEDULE_TIMELINES_REL]->(tl:ScheduleTimeline)<-[r4:TIMELINE_REL]-(act_main)<-[r5:ACTIVITY_REL]-(act_inst_main:ScheduledActivityInstance)
        MATCH(tl)-[r6:INSTANCES_REL]->(act_inst:ScheduledActivityInstance)-[r7:ACTIVITY_REL]->(act:Activity)-[r8:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)-[r9:PROPERTIES_REL]->(bc_prop:BiomedicalConceptProperty)<-[r10:PROPERTIES_REL]-(dc:DataContract)-[r11:INSTANCES_REL]->(act_inst_main),(tl)-[r12:TIMINGS_REL]->(tpt:Timing)-[r13:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]->(act_inst)<-[:INSTANCES_REL]-(dc)
        MATCH(act_inst_main)-[r14:ENCOUNTER_REL]->(e:Encounter)
        return distinct sd, 
        tl, 
        e,
        tpt,
        act, 
        act_inst_main,
        act_inst,  
        bc,
        bc_prop,
        dc",{s:'%s'}) YIELD value
        WITH value.sd as sd, 
        value.tl as tl,
        value.e as e,
        value.tpt as tpt, 
        value.act as act, 
        value.act_inst_main as act_inst_main, 
        value.act_inst as act_inst, 
        value.bc as bc, 
        value.bc_prop as bc_prop, 
        value. dc as dc
        WITH sd,tl,e,tpt,act,act_inst_main,act_inst,bc,bc_prop,dc 
          return act.label as activity,
            e.label as visit,
            tpt.value as timepoint,
            bc.name as bc_name,
            bc_prop.name as property,
            tl.name as timeline,
            dc.uri as uri order by toInteger(substring(e.name,1)), activity, bc_name, property %s 
        """ % (uuid, skip_offset_clause)
      #print(f"QUERY 2: {query}")
      result = session.run(query)
      results = []
      for record in result:
        final_record = { 
          "visit": record["visit"], 
          "timepoint":record["timepoint"],
          "activity": record["activity"], 
          "bc_name": record["bc_name"], 
          "property": record["property"],
          "timeline": record["timeline"],
          "uri": record["uri"] 
        }
        results.append(final_record)
    result = {'items': results, 'page': page, 'size': size, 'filter': filter, 'count': count }
    return result

  @classmethod
  def _parse_name(cls, name):
    return re.sub('[^0-9a-zA-Z]+', '-', name.lower())

  @staticmethod
  def _set_data_contract(tx, name, parsed_name, uri_root):
    query= """
      call apoc.cypher.run("MATCH(study:Study{name:$s})-[r1:VERSIONS_REL]->(StudyVersion)-[r2:STUDY_DESIGNS_REL]->(sd:StudyDesign)
      MATCH(sd)-[r3:SCHEDULE_TIMELINES_REL]->(tl:ScheduleTimeline) where not (tl)<-[:TIMELINE_REL]-()
      MATCH(tl)-[r4:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)-[r5:ACTIVITY_REL]->(act:Activity)-[r6:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)-[r7:PROPERTIES_REL]->(bc_prop:BiomedicalConceptProperty) 
      return distinct study, 
      sd, 
      tl, 
      act, 
      act_inst_main,
      act_inst_main.uuid as act_inst_uuid,
      null as act_inst,  
      bc,
      bc_prop
            UNION
      MATCH(study:Study{name:$s})-[r1:VERSIONS_REL]->(StudyVersion)-[r2:STUDY_DESIGNS_REL]->(sd:StudyDesign)
      MATCH(sd)-[r3:SCHEDULE_TIMELINES_REL]->(tl:ScheduleTimeline)<-[r4:TIMELINE_REL]-(act_main)<-[r5:ACTIVITY_REL]-(act_inst_main:ScheduledActivityInstance)
      MATCH(tl)-[r6:INSTANCES_REL]->(act_inst:ScheduledActivityInstance)-[r7:ACTIVITY_REL]->(act:Activity)-[r8:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)-[r9:PROPERTIES_REL]->(bc_prop:BiomedicalConceptProperty) 
      return distinct study, 
      sd, 
      tl, 
      act, 
      act_inst_main,
      act_inst_main.uuid+'/'+ act_inst.uuid as act_inst_uuid,
      act_inst,  
      bc,
      bc_prop",{s:'%s'}) YIELD value
      WITH value.study as study, 
      value.sd as sd, 
      value.tl as tl, 
      value.act as act, 
      value.act_inst_main as act_inst_main, 
      value.act_inst as act_inst, 
      value.bc as bc, 
      value.bc_prop as bc_prop, 
      value. act_inst_uuid as  act_inst_uuid
      WITH study, sd, tl,act,act_inst_main,act_inst,bc,bc_prop,act_inst_uuid
      MERGE (dc:DataContract{uri:'%s' + '%s' + '/' + act_inst_uuid + '/' + bc_prop.uuid})
            MERGE (dc)-[:PROPERTIES_REL]->(bc_prop)
            MERGE (dc)-[:INSTANCES_REL]->(act_inst)
            MERGE (dc)-[:INSTANCES_REL]->(act_inst_main)
            SET study.uri = '%s' + '%s'
    """ % (name, uri_root, parsed_name, uri_root, parsed_name)
    print(f"SDC QUERY: {query}")
    results = tx.run(query)
    #for row in results:
    #  return StudyFile.wrap(row['sf'])
    return True