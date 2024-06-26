from uuid import uuid4
from d4kms_generic import application_logger
from d4kms_service import Neo4jConnection
from model.base_node import Node
from model.code import Code
from model.alias_code import AliasCode
from model.biomedical_concept import BiomedicalConcept
from model.biomedical_concept_property import BiomedicalConceptProperty
from model.biomedical_concept import BiomedicalConceptSimple
from model.response_code import ResponseCode
from model.crm import CRMNode

class StudyDesignBC():

  @classmethod
  def fix(cls, name):
    results = {}
    study_design = cls._get_study_design(name)
    bcs = cls._get_bcs(study_design)
    for bc in bcs:
      results[bc.name] = cls._add_propety(bc)
      application_logger.info(f"Inserted --DTC for '{bc.name}'")   
    return results
  
  @classmethod
  def create(cls, name):
    results = {}
    study_design = cls._get_study_design(name)
    properties = cls._get_properties(study_design)
    crm_nodes = cls._get_crm()
    crm_map = {}
    for uri, node in crm_nodes.items():
      vars = node.sdtm.split(',')
      for var in vars:
        if var not in crm_map:
          crm_map[var] = []  
        crm_map[var].append(node)
    #print(f"MAP {crm_map}")
    #print(f"PROPERTIES: {[i.name for i in properties]}")
    for p in properties:
      #print(f"P: {p.name}")
      nodes = cls._match(p.name, crm_map)
      #print(f"N: {nodes}")
      if nodes:
        if len(nodes) == 1:
          node = nodes[0]
        else:
          if p.name.endswith('ORRES') and p.responseCodes:
            node = next((i for i in nodes if i.datatype == 'coded'), None)
          else:
            node = next((i for i in nodes if i.datatype == 'quantity'), None)
        if node:
          p.relationship(nodes[0], 'IS_A_REL')                
          application_logger.info(f"Linked BC property '{p.name}' -> '{node.uri}'")   
          results[p.name] = node.uri
        else:
          application_logger.error(f"Failed to link BC property '{p.name}', nodes '{nodes}' detected but no match")
      else:
        application_logger.error(f"Failed to link BC property '{p.name}', no nodes detected")
    return results

  @classmethod
  def unlinked(cls, uuid, page, size, filter):
    skip_offset_clause = ""
    if page != 0:
      offset = (page - 1) * size
      skip_offset_clause = "SKIP %s LIMIT %s" % (offset, size)
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[]->(bc:BiomedicalConcept) WHERE NOT (bc)<-[:USING_BC_REL]-()
        RETURN COUNT(DISTINCT(bc.name)) AS count
      """ % (uuid)
      #print(f"QUERY: {query}")
      result = session.run(query)
      count = 0
      for record in result:
        count = record['count']
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[]->(bc:BiomedicalConcept) WHERE NOT (bc)<-[:USING_BC_REL]-()
        RETURN DISTINCT(bc.name) as name ORDER BY name %s
      """ % (uuid, skip_offset_clause)
      #print(f"QUERY: {query}")
      result = session.run(query)
      results = []
      for record in result:
        results.append(record['name'])
    result = {'items': results, 'page': page, 'size': size, 'filter': filter, 'count': count }
    return result
  
  @staticmethod
  def _match(name, map):
    name_upper = name.upper()
    generic_name = f"--{name[2:]}".upper()
    # if name_upper == "SEX":
    #   print(f"Match {name} {generic_name}")
    #   print(f"keys {map.keys()}")
    if name_upper in map.keys():
      #print(f"Matched full {map[name].uri}")
      return map[name_upper]
    elif generic_name in map.keys():
      #print(f"Matched generic {map[generic_name].uri}")
      return map[generic_name]
    return None

  @staticmethod
  def _get_study_design(name):
    
    from model.study_design import StudyDesign
    
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign {name: '%s'}) return sd
      """ % (name)
      result = session.run(query)
      for record in result:
        return StudyDesign.wrap(record['sd'])
      return None

  @staticmethod
  def _get_crm():
    db = Neo4jConnection()
    with db.session() as session:
      results = {}
      query = """
        MATCH (n:CRMNode) return n
      """ 
      records = session.run(query)
      for record in records:
        crm_node = CRMNode.wrap(record['n'])
        results[crm_node.uri] = crm_node
      return results
    
  @staticmethod
  def _get_properties(study_design):
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[:BIOMEDICAL_CONCEPTS_REL]->(bc:BiomedicalConcept)-[]->(bcp:BiomedicalConceptProperty)
        WITH bcp
        OPTIONAL MATCH (bcp)-[]->(rc:ResponseCode)-[]->(c:Code) 
        RETURN DISTINCT bcp, rc, c
      """ % (study_design.uuid)
      result = session.run(query)
      p_map = {}
      for record in result:
        if record['rc']:
          rc_params = Node.as_dict(record['rc'])
          rc_params['code'] = Code.wrap(record['c'])
          rc = ResponseCode.wrap(rc_params)
        else:
          rc = None
        p_params = Node.as_dict(record['bcp'])
        uuid = p_params['uuid']
        if uuid in p_map:
          p = p_map[uuid]
        else:
          code = Code(**{'uuid': 'uuid', 'id': '0', 'code': '1', 'codeSystem': '2', 'codeSystemVersion': '3', 'decode': '4', 'instanceType': 'Code'})
          p_params['code'] = AliasCode(**{'uuid': 'uuid', 'id': '0', 'standardCode': code, 'instanceType': 'AliasCode'})
          p = BiomedicalConceptProperty.wrap(p_params)
          p_map[uuid] = p
          results.append(p)
        if rc:
          p.responseCodes.append(rc)
      return results

  @staticmethod
  def _get_bcs(study_design):
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[:BIOMEDICAL_CONCEPTS_REL]->(bc:BiomedicalConcept)
        RETURN DISTINCT bc
      """ % (study_design.uuid)
      result = session.run(query)
      for record in result:
        results.append(BiomedicalConceptSimple.wrap(record['bc']))
      return results

  @staticmethod
  def _add_propety(bc):
    uuids = {'property': str(uuid4()), 'code': str(uuid4()), 'aliasCode': str(uuid4())}
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        MATCH (bc:BiomedicalConcept {uuid: '%s'})
        WITH bc
        CREATE (c:Code {uuid: $s_uuid1, id: 'tbd', code: 'tbd', codeSystem: 'http://www.cdisc.org', codeSystemVersion: '2023-09-29', decode: 'tbd', instanceType: 'Code'})
        CREATE (ac:AliasCode {uuid: $s_uuid2, id: 'tbd', instanceType: 'AliasCode'})
        CREATE (p:BiomedicalConceptProperty {uuid: $s_uuid3, id: 'tbd', name: '--DTC', label: 'Date Time', isRequired: 'true', isEnabled: 'true', datatype: 'datetime', instanceType: 'BiomedicalConceptProperty'})
        CREATE (bc)-[:PROPERTIES_REL]->(p)-[:CODE_REL]->(ac)-[:STANDARD_CODE_REL]->(c)
        RETURN p.uuid as uuid
      """ % (bc.uuid)
      result = session.run(query, 
        s_uuid1=str(uuid4()), 
        s_uuid2=str(uuid4()), 
        s_uuid3=str(uuid4())
      )
      for row in result:
        return uuids
