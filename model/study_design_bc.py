from d4kms_generic import application_logger
from d4kms_service import Neo4jConnection
from model.base_node import Node
from model.code import Code
from model.alias_code import AliasCode
from model.biomedical_concept_property import BiomedicalConceptProperty
from model.response_code import ResponseCode
from model.crm import CRMNode

class StudyDesignBC():

  @classmethod
  def create(cls, name):
    results = {}
    study_design = cls._get_study_design(name)
    properties = cls._get_properties_by_name(study_design)
    crm_nodes = cls._get_crm()
    crm_map = {}
    for uri, node in crm_nodes.items():
      vars = node.sdtm.split(',')
      for var in vars:
        if var not in crm_map:
          crm_map[var] = []  
        crm_map[var].append(node)
    #print(f"MAP {crm_map}")
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
  
  @staticmethod
  def _match(name, map):
    generic_name = f"--{name[2:]}"
    #print(f"Match {name} {generic_name}")
    if name in map.keys():
      #print(f"Matched full {map[name].uri}")
      return map[name]
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
  def _get_properties_by_name(study_design):
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
