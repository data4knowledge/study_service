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
      print(f"QUERY 1: {query}")
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
        MATCH(sd:StudyDesign {uuid: '%s'})
        OPTIONAL MATCH(sd)-[:ACTIVITIES_REL]-(act:Activity)
        OPTIONAL MATCH(act)<-[:ACTIVITY_REL]-(act_inst:ScheduledActivityInstance)<-[:INSTANCES_REL]-(tl:ScheduleTimeline)
        OPTIONAL MATCH(act)-[:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)
        OPTIONAL MATCH(bc)-[:PROPERTIES_REL]->(bc_prop:BiomedicalConceptProperty)
        MATCH(dc:DataContract)-[:PROPERTIES_REL]->(bc_prop)
        MATCH(dc)-[:INSTANCES_REL]->(act_inst)
        MATCH(act_inst)-[:ENCOUNTER_REL]->(e:Encounter)
        RETURN 
          act.label as activity,
          e.label as visit,
          bc.name as bc_name,
          bc_prop.name as property,
          tl.name as timeline,
          dc.uri as uri order by visit, activity, bc_name, property %s 
        """ % (uuid, skip_offset_clause)
      print(f"QUERY 2: {query}")
      result = session.run(query)
      results = []
      for record in result:
        final_record = { 
          "visit": record["visit"], 
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
      MATCH(s:Study{name:'%s'})-[:VERSIONS_REL]->(StudyVersion)-[:STUDY_DESIGNS_REL]->(sd:StudyDesign)
      OPTIONAL MATCH (sd)-[:ACTIVITIES_REL]-(act:Activity)
      OPTIONAL MATCH (act)<-[:ACTIVITY_REL]-(act_inst:ScheduledActivityInstance)<-[:INSTANCES_REL]-(tl:ScheduleTimeline)
      OPTIONAL MATCH (act)-[:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)
      MATCH (bc)-[:PROPERTIES_REL]->(bc_prop:BiomedicalConceptProperty)
      WITH s, sd, act, tl, bc, act_inst, bc_prop
      MERGE (dc:DataContract{uri:'%s' + '%s' + '/' + act_inst.uuid + '/' + bc_prop.uuid})
      MERGE (dc)-[:PROPERTIES_REL]->(bc_prop)
      MERGE (dc)-[:INSTANCES_REL]->(act_inst)
      SET s.uri = '%s' + '%s'
    """ % (name, uri_root, parsed_name, uri_root, parsed_name)
    results = tx.run(query)
    #for row in results:
    #  return StudyFile.wrap(row['sf'])
    return True