import re
import yaml
from yattag import Doc
#from bs4 import BeautifulSoup   
from typing import List, Literal
from model.base_node import *
from model.code import Code
from model.governance_date import GovernanceDate
from model.narrative_content import NarrativeContent
#from .section_number import SectionNumber
#from .element.element import Element
from model.element.element_manager import ElementManager
from model.template.template_manager import template_manager, TemplateManager
from model.template.template_definition import TemplateDefinition
from d4kms_generic import *
from d4kms_service import *
from uuid import uuid4
#from .utility.utility import read_yaml_file

class SPDVBackground():

  def __init__(self, study_version):
    self._index = 0
    #self._template_manager = TemplateManager()

  def add_all_sections(self, uuids, template_uuid):

    from model.study_version import StudyVersion

    self._index = 1
    cypher = []
    cypher.append("MATCH (spdv:StudyProtocolDocumentVersion {uuid: '%s'})\nWITH spdv" % (uuids['StudyProtocolDocumentVersion']))
    study_version = StudyVersion.find(uuids['StudyVersion'])
    template = template_manager.template(template_uuid, study_version)
    self.add_section_cypher(cypher, template.section_hierarchy(), template)
    db = Neo4jConnection()
    with db.session() as session:
      #print(f"CYPHER: {cypher}")
      session.run("\n".join(cypher))

  def add_section_cypher(self, cypher, section, template: TemplateDefinition):
    uuid = uuid4()
    node_label = f"nc{self._index}"
    self._index += 1
    section_item = section['item']
    #print(f"SECTION ITEM: {section_item}")
    section_definition = template.section_definition(section_item['uuid']) if section_item['uuid'] else None
    #print(f"SECTION DEF: {section_definition}")
    section_text = section_definition.resolve() if section_definition else '<div></div>'
    query = """
      CREATE (%s:NarrativeContent {id: '%s', name: '%s', description: '', label: '', sectionNumber: '%s', sectionTitle: '%s', text: '%s', uuid: '%s', instanceType: 'NarrativeContent'})
      CREATE (spdv)-[:CONTENTS_REL]->(%s)
    """ % (node_label, f"SECTION_{section_item['section_number']}", f"SECTION_{section_item['section_number']}", section_item['section_number'], section_item['section_title'], section_text, uuid, node_label)
    cypher.append(query)
    for child in section['children']:
      child_label = self.add_section_cypher(cypher, child, template)
      query = "CREATE (%s)-[:CHILD_REL]->(%s)" % (node_label, child_label)
      cypher.append(query)
    return node_label

class StudyProtocolDocumentVersion(NodeId):
  protocolVersion: str
  protocolStatus: Code = None
  dateValues: List[GovernanceDate] = []
  contents: List[NarrativeContent] = []
  childIds: List[str] = []
  instanceType: Literal['StudyProtocolDocumentVersion']
  templateUuid: str = None

  #from model.study_version import StudyVersion

  def __init__(self, *a, **kw):
    super().__init__(*a, **kw)
    self._study_version = None
    self._set_study_version()
    #self._template_manager = TemplateManager()
    
  @classmethod
  def find_from_study(cls, uuid):
    db = Neo4jConnection()
    with db.session() as session:
      return session.execute_read(cls._find_from_study, uuid)

  def document_definition(self):
    result = []
    template = self._get_template()
    items = self._all_narrative_content()
    for section in self.section_list()['root']:
      print(f"SECTION: {section}")
      definition = template.section_definition(section['uuid'])
      nc_uuid = items[definition.section_number].uuid if definition.section_number in items else None
      result.append({'definition': definition, 'data': nc_uuid})
    return {'items': result}

  def section_definition(self, uuid):
    template = template_manager.template(self.templateUuid, self._study_version)
    section_def = template.section_definition(uuid)
    #print(f"SECTION DEF: {section_def}")
    nc = self._narrative_content_get(section_def.section_number)
    return {'definition': section_def, 'data': nc.uuid}

  def section_read(self, uuid):
    #template = template_manager.template(self.templateUuid, self._study_version)
    nc = NarrativeContent.find(uuid)
    #section_def = template.section_definition_by_section_number(nc.sectionNumber)
    return nc.to_html(self._study_version) if nc else ''

  def section_write(self, uuid, text):
    try:
      result = self._narrative_content_post(uuid, text)
      return {'uuid': result}  
    except Exception as e:
      application_logger.exception(f"Exception raised while writing to section", e, UnexpectedError)

  def section_list(self):
    template = self._get_template()
    result = {'root': template.section_list()}
    return result

  def _get_template(self):
    try:
      template = template_manager.template(self.templateUuid, self._study_version)
    except TemplateManager.MissingTemplate as e:
      items = self._all_narrative_content()
      #print(f"ITEMS: {items}")
      self.templateUuid, template = template_manager.template_from_sections(items, self._study_version)
      print(f"SELF: {self}")
      self.set('templateUuid')
    return template

  def element(self, name):
    element_manager = ElementManager(self._study_version)
    return element_manager.element(name).definition()

  def element_read(self, name):
    try:
      result = ElementManager(self._study_version).element(name).read()
      return result
    except Exception as e:
      application_logger.exception(f"Exception raised while reading from  element", e, UnexpectedError)

  def element_write(self, name, text):
    try:
      result = ElementManager(self._study_version).element(name).write(text)
      return result
    except Exception as e:
      application_logger.exception(f"Exception raised while writing to element", e, UnexpectedError)
  
  def _set_study_version(self):
    db = Neo4jConnection()
    with db.session() as session:
      result = session.execute_read(self._find_from_spdv, self.uuid)
      if not 'error' in result:
        self._study_version = result['result']

  def _narrative_content_get(self, section):
    db = Neo4jConnection()
    with db.session() as session:
      return session.execute_read(self._narrative_content_read, self.uuid, section)

  def _narrative_content_post(self, uuid, text):
    db = Neo4jConnection()
    with db.session() as session:
      if session.execute_read(self._section_exists, self.uuid, uuid):
        return session.execute_write(self._narrative_content_write, self.uuid, uuid, text)
      else:
        application_logger.warning(f"Missing section {uuid} detected")
        return None

  def _all_narrative_content(self):
    db = Neo4jConnection()
    with db.session() as session:
      return session.execute_read(self._all_narrative_content_read, self.uuid)

  @staticmethod
  def _find_from_spdv(tx, uuid):
    query = "MATCH (sv:StudyVersion)-[:DOCUMENT_VERSION_REL]->(spdv:StudyProtocolDocumentVersion {uuid: $uuid}) RETURN sv"
    result = tx.run(query, uuid=uuid)
    for row in result:
      return {'result': NodeId.wrap(row['sv'])}
    return {'error': f"Exception. Failed to find study version"}

  @staticmethod
  def _find_from_study(tx, uuid):
    query = "MATCH (s:Study {uuid: $uuid})-[:DOCUMENTED_BY_REL]->(spd:StudyProtocolDocument)-[:VERSIONS_REL]->(spdv:StudyProtocolDocumentVersion) RETURN spdv"
    result = tx.run(query, uuid=uuid)
    for row in result:
      return StudyProtocolDocumentVersion.wrap(row['spdv'])
    return None
  
  @staticmethod
  def _narrative_content_read(tx, uuid, section):
    query = """
      MATCH (spdv:StudyProtocolDocumentVersion {uuid: $uuid1})-[:CONTENTS_REL]->(nc:NarrativeContent {sectionNumber: $section}) RETURN nc
    """
    rows = tx.run(query, uuid1=uuid, section=section)
    results = {}
    for item in rows:
      return NarrativeContent.wrap(item['nc'])
    return None

  @staticmethod
  def _narrative_content_write(tx, uuid, section_uuid, text):
    query = """
      MATCH (spdv:StudyProtocolDocumentVersion {uuid: $uuid1})-[:CONTENTS_REL]->(nc:NarrativeContent {uuid: $section_uuid})
      SET nc.text = $text
      RETURN nc.uuid as uuid
    """
    rows = tx.run(query, uuid1=uuid, section_uuid=section_uuid, text=text)
    for row in rows:
      return row['uuid']
    return None
  
  @staticmethod
  def _all_narrative_content_read(tx, uuid):
    #print(f"NC ALL {uuid}")
    query = """
      MATCH (spdv:StudyProtocolDocumentVersion {uuid: $uuid1})-[:CONTENTS_REL]->(nc:NarrativeContent) RETURN nc
    """
    rows = tx.run(query, uuid1=uuid)
    results = {}
    for item in rows:
      nc = NarrativeContent.wrap(item['nc'])
      nc.sectionNumber = '0' if nc.sectionNumber == '' else nc.sectionNumber
      results[nc.sectionNumber] = nc
    return results
  
  @staticmethod
  def _section_exists(tx, uuid, section_uuid):
    query = "MATCH (spdv:StudyProtocolDocumentVersion {uuid: $uuid1})-[:CONTENTS_REL]->(nc:NarrativeContent {uuid: $uuid2}) RETURN nc"
    result = tx.run(query, uuid1=uuid, uuid2=section_uuid)
    return True if result.peek() else False
