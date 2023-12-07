from uuid import uuid4
from .neo4j_connection import Neo4jConnection

index = 0

def add_all_sections(sections, uuid):
  
  global index

  index = 1
  cypher = []
  cypher.append("MATCH (spdv:StudyProtocolDocumentVersion {uuid: '%s'})\nWITH spdv" % (uuid))
  add_section_cypher(cypher, sections)
  db = Neo4jConnection()
  with db.session() as session:
    session.run("\n".join(cypher))

def add_section_cypher(cypher, section):
  
  global index
  
  print(f"SECTION {section}")
  uuid = uuid4()
  node_label = f"nc{index}"
  index += 1
  query = """
    CREATE (%s:NarrativeContent {id: '%s', name: '%s', description: '', label: '', sectionNumber: '%s', sectionTitle: '%s', text: '', uuid: '%s'})
    CREATE (spdv)-[:CONTENTS_REL]->(%s)
  """ % (node_label, f"SECTION_{section['item']['section_number']}", f"SECTION_{section['item']['section_number']}", section['item']['section_number'], section['item']['section_title'], uuid, node_label)
  cypher.append(query)
  for child in section['children']:
    child_label = add_section_cypher(cypher, child)
    query = "CREATE (%s)-[:CHILDREN_REL]->(%s)" % (node_label, child_label)
    cypher.append(query)
  return node_label
