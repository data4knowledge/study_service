import re
import yaml
from yattag import Doc
from bs4 import BeautifulSoup   
from typing import List, Literal
from .base_node import *
from .code import Code
from .governance_date import GovernanceDate
from .narrative_content import NarrativeContent
from .section_number import SectionNumber
from .element.element import Element
from d4kms_generic import *
from d4kms_service import *
from uuid import uuid4

class SPDVBackground():

  def __init__(self):
    self.index = 0

  def add_all_sections(self, sections, uuid):
    self.index = 1
    cypher = []
    cypher.append("MATCH (spdv:StudyProtocolDocumentVersion {uuid: '%s'})\nWITH spdv" % (uuid))
    self.add_section_cypher(cypher, sections)
    db = Neo4jConnection()
    with db.session() as session:
      session.run("\n".join(cypher))

  def add_section_cypher(self, cypher, section):
    uuid = uuid4()
    node_label = f"nc{self.index}"
    self.index += 1
    query = """
      CREATE (%s:NarrativeContent {id: '%s', name: '%s', description: '', label: '', sectionNumber: '%s', sectionTitle: '%s', text: '%s', uuid: '%s'})
      CREATE (spdv)-[:CONTENTS_REL]->(%s)
    """ % (node_label, f"SECTION_{section['item']['section_number']}", f"SECTION_{section['item']['section_number']}", section['item']['section_number'], section['item']['section_title'], section['item']['text'], uuid, node_label)
    cypher.append(query)
    for child in section['children']:
      child_label = self.add_section_cypher(cypher, child)
      query = "CREATE (%s)-[:CHILDREN_REL]->(%s)" % (node_label, child_label)
      cypher.append(query)
    return node_label

class StudyProtocolDocumentVersion(NodeId):
  protocolVersion: str
  protocolStatus: Code = None
  dateValues: List[GovernanceDate] = []
  contents: List[NarrativeContent] = []
  childIds: List[str] = []
  instanceType: Literal['StudyProtocolDocumentVersion']

  def document_as_html(self):
    try:
      doc = Doc()
      with doc.tag('body'):
        #doc.asis(self._front_sheet(self.briefTitle))    
        items = self._all_narrative_content()
        print(f"ITEMS: {items}")
        for section in self.section_list()['root']:
          try:
            print(f"SECTION: {section}")
            content = items[section['key']]
          except Exception as e:
            application_logger.warning(f"Protocol document section {section['key']} not found")
            uuid = str(uuid4())
            content = NarrativeContent(
              id=uuid, 
              uuid=uuid, 
              name=f"SECTION_{section['section_number']}", 
              sectionNumber=section['section_number'], 
              sectionTitle=section['section_title'], 
              text=""
            )
          self._content_to_html(content, doc)
      return doc.getvalue()
    except Exception as e:
      application_logger.exception(f"Exception raised while building protocol document", e, UnexpectedError)

  def section_as_html(self, section_number):
    try:
      doc = Doc()
      try:
        content = self._narrative_content_get(section_number)
        #print(f"SECTION: {content}, {section_number}")
      except Exception as e:
        application_logger.warning(f"Protocol document section {section_number} not found")
        uuid = str(uuid4())
        section_def = self._read_section_definition(section_number)
        content = NarrativeContent(
          id=uuid, 
          uuid=uuid, 
          name=f"SECTION_{section_number}", 
          sectionNumber=section_def['section_number'], 
          sectionTitle=section_def['section_title'], 
          text=""
        )
      self._content_to_html(content, doc)
      return doc.getvalue()
    except Exception as e:
      application_logger.exception(f"Exception raised while building protocol document section", e, UnexpectedError)

  @classmethod
  def find_from_study(cls, uuid):
    db = Neo4jConnection()
    with db.session() as session:
      return session.execute_read(cls._find_from_study, uuid)

  @staticmethod
  def _find_from_study(tx, uuid):
    query = "MATCH (s:Study {uuid: $uuid})-[:DOCUMENTED_BY_REL]->(spd:StudyProtocolDocument)-[:VERSIONS_REL]->(spdv:StudyProtocolDocumentVersion) RETURN spdv"
    result = tx.run(query, uuid=uuid)
    for row in result:
      return StudyProtocolDocumentVersion.wrap(row['spdv'])
    return None
  
  def section(self, key):
    section_def = self._read_section_definition(key)
    nc = self._narrative_content_get(key)
    section_def['text'] = nc.text if nc else ''
    return section_def

  def element(self, key):
    return self._read_element_definition(key)

  def section_write(self, key, text):
    try:
      result = self._narrative_content_post(key, text)
      return {'uuid': result}  
    except Exception as e:
      application_logger.exception(f"Exception raised while writing to section", e, UnexpectedError)

  def element_read(self, study_version, key):
    try:
      result = Element(study_version, key).read()
      return result
    except Exception as e:
      application_logger.exception(f"Exception raised while reading from  element", e, UnexpectedError)

  def element_write(self, study_version, key, text):
    try:
      result = Element(study_version, key).write(text)
      return result
    except Exception as e:
      application_logger.exception(f"Exception raised while writing to element", e, UnexpectedError)

  def section_list(self):
    section_defs = self._read_section_definitions()
    sections = self._read_section_list()
    for section in sections:
      section['header_only'] = section_defs[section['key']]['header_only']
    result = {'root': sections}
    return result
  
  def section_hierarchy(self):
    section_defs = self._read_section_definitions()
    sections = self._read_section_list()
    parent = [{'item': {'name':	'ROOT', 'section_number': '-1', 'section_title':	'Root', 'text': ''}, 'children': []}]
    current_level = 1
    previous_item = None
    for section in sections:
      section_number = SectionNumber(section['section_number'])
      section['header_only'] = section_defs[section['key']]['header_only']
      item = {'item': section, 'children': []}
      if section_number.level == current_level:
        parent[-1]['children'].append(item)
      elif section_number.level > current_level:
        parent.append(previous_item)
        parent[-1]['children'].append(item)
        current_level = section_number.level
      elif section_number.level < current_level:
        parent.pop()
        parent[-1]['children'].append(item)
        current_level = section_number.level
      current_level = section_number.level
      previous_item = item
    return parent[0]

  def _read_section_definition(self, key):
    data = self._read_as_yaml_file("data/m11_to_usdm.yaml")
    return data[key]

  def _read_element_definition(self, key):
    data = self._read_as_yaml_file("data/m11_elements.yaml")
    return data[key]

  def _read_section_definitions(self):
    return self._read_as_yaml_file("data/m11_to_usdm.yaml")

  def _read_section_list(self):
    return self._read_as_yaml_file("data/m11_sections.yaml")
  
  def _read_as_yaml_file(self, filepath):
    with open(filepath, "r") as f:
      data = yaml.load(f, Loader=yaml.FullLoader)
    return data
  
  def _narrative_content_get(self, section):
    db = Neo4jConnection()
    with db.session() as session:
      return session.execute_read(self._narrative_content_read, self.uuid, section)

  def _narrative_content_post(self, section, text):
    db = Neo4jConnection()
    with db.session() as session:
      if session.execute_read(self._section_exists, self.uuid, section):
        return session.execute_write(self._narrative_content_write, self.uuid, section, text)
      else:
        application_logger.warning(f"Missing section {section} detected")
        return None

  def _all_narrative_content(self):
    db = Neo4jConnection()
    with db.session() as session:
      return session.execute_read(self._all_narrative_content_read, self.uuid)

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
  def _narrative_content_write(tx, uuid, section, text):
    query = """
      MATCH (spdv:StudyProtocolDocumentVersion {uuid: $uuid1})-[:CONTENTS_REL]->(nc:NarrativeContent {sectionNumber: $section})
      SET nc.text = $text
      RETURN nc.uuid as uuid
    """
    rows = tx.run(query, uuid1=uuid, section=section, text=text)
    for row in rows:
      print(f"NC WRITE {uuid}, {section} {row['uuid']}")
      return row['uuid']
    return None
  
  @staticmethod
  def _all_narrative_content_read(tx, uuid):
    print(f"NC ALL {uuid}")
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
  def _section_exists(tx, uuid, section):
    query = "MATCH (spdv:StudyProtocolDocumentVersion {uuid: $uuid1})-[:CONTENTS_REL]->(nc:NarrativeContent {sectionNumber: $section}) RETURN nc"
    result = tx.run(query, uuid1=uuid, section=section)
    return True if result.peek() else False

  def _content_to_html(self, content, doc):
    level = content.level()
    klass = "page" if level == 1 else ""
    heading_id = f"section-{content.sectionNumber}"
    with doc.tag('div', klass=klass):
      try:
        section_number = SectionNumber(content.sectionNumber)
        if not section_number.title_sheet:
          with doc.tag(f'h{level + 4}', id=heading_id):
            doc.asis(f"{content.sectionNumber}&nbsp{content.sectionTitle}")
        if self._standard_section(content.text):
          name = self._standard_section_name(content.text)
          content.text = self._generate_standard_section(name)
        doc.asis(self._translate_references(content.text))
      except Exception as e:
        application_logger.exception(f"Exception raised while creating HTML from content", UnexpectedError)

  def _translate_references(self, content_text):
    soup = BeautifulSoup(content_text, 'html.parser')
    for ref in soup(['usdm:ref']):
      attributes = ref.attrs
      if 'namexref' in attributes:
        application_logger.error(f"Xref reference detected {attributes}", e)
      else:
        instance = Node.find(attributes['uuid'], True)
      try:
        value = str(instance[attributes['attribute']])
        translated_text = self._translate_references(value)
        ref.replace_with(translated_text)
      except Exception as e:
        application_logger.exception(f"Failed to translate reference, attributes {attributes}", e)
        ref.replace_with('Missing content')
    return str(soup)
  
  def _standard_section(self, text):
    soup = BeautifulSoup(text, 'html.parser')
    for section in soup(['usdm:section']):
      return True
    return False
  
  def _standard_section_name(self, text):  
    soup = BeautifulSoup(text, 'html.parser')
    for section in soup(['usdm:section']):
      attributes = section.attrs
      if 'name' in attributes:
        return attributes['name'].upper()
      else:
        return None
    return None

  def _generate_standard_section(self, name):
    #print(f"GSS: {name}")   
    if name == "M11-TITLE-PAGE":
      return self._generate_m11_title_page()
    elif name == "M11-INCLUSION":
      return self._generate_m11_criteria("C25532")
    elif name == "M11-EXCLUSION":
      return self._generate_m11_criteria("C25370")
    elif name == "M11-OBJECTIVE-ENDPOINTS":
      return self._generate_m11_objective_endpoints()
    else:
      return f"Unrecognized standard content name {name}"

  def _generate_m11_title_page(self):
    print(f"M11 TP:")
    doc = Doc()
    with doc.tag('table'):
      self._generate_m11_title_page_entry(doc, 'Sponsor Confidentiality Statement:', '')
      self._generate_m11_title_page_entry(doc, 'Full Title:', f'{self._study_full_title()}')
      self._generate_m11_title_page_entry(doc, 'Trial Acronym:', f'{self._study_acronym()}')
      self._generate_m11_title_page_entry(doc, 'Protocol Identifier:', f'{self._study_identifier()}')
      self._generate_m11_title_page_entry(doc, 'Original Protocol:', '')
      self._generate_m11_title_page_entry(doc, 'Version Number:', f'{self._study_version()}')
      self._generate_m11_title_page_entry(doc, 'Version Date:', f'{self._study_date()}')
      self._generate_m11_title_page_entry(doc, 'Amendment Identifier:', f'{self._amendment()}')
      self._generate_m11_title_page_entry(doc, 'Amendment Scope:', f'{self._amendment_scopes()}')
      self._generate_m11_title_page_entry(doc, 'Compound Codes(s):', '')
      self._generate_m11_title_page_entry(doc, 'Compound Name(s):', '')
      self._generate_m11_title_page_entry(doc, 'Trial Phase:', f'{self._study_phase()}')
      self._generate_m11_title_page_entry(doc, 'Short Title:', f'{self._study_short_title()}')
      self._generate_m11_title_page_entry(doc, 'Sponsor Name and Address:', f'{self._organization_name_and_address()}')
      self._generate_m11_title_page_entry(doc, 'Regulatory Agency Identifier Number(s):', f'{self._study_regulatory_identifiers()}')
      self._generate_m11_title_page_entry(doc, 'Spondor Approval Date:', f'{self._approval_date()}')

      # Enter Nonproprietary Name(s)
      # Enter Proprietary Name(s)
      # Globally/Locally/Cohort
      # Primary Reason for Amendment
      # Region Identifier
      # Secondary Reason for Amendment

    result = doc.getvalue()
    #print(f"DOC: {result}")
    return result
  
  def _generate_m11_criteria(self, type):
    #print(f"M11 TP:")
    heading = { 
      'C25532': "Patients may be included in the study only if they meet <strong>all</strong> the following criteria:",
      'C25370': "Patients may be excluded in the study for <strong>any</strong> of the following reasons:",
    }
    doc = Doc()
    with doc.tag('p'):
      doc.asis(heading[type])  
    with doc.tag('table'):
      for criterion in self._criteria(type):
        self._generate_m11_critieria_entry(doc, criterion['identifier'], criterion['text'])
    return doc.getvalue()

  def _generate_m11_objective_endpoints(self):
    #print(f"M11 TP:")
    doc = Doc()
    with doc.tag('table'):
      for item in self._objective_endpoints():
        self._generate_m11_objective_endpoints_entry(doc, item['objective'], item['endpoints'])
    return doc.getvalue()

  def _generate_m11_critieria_entry(self, doc, number, entry):
    with doc.tag('tr'):
      with doc.tag('td', style="vertical-align: top; text-align: left"):
        with doc.tag('p'):
          doc.asis(number)  
      with doc.tag('td', style="vertical-align: top; text-align: left"):
        with doc.tag('p'):
          doc.asis(entry)

  def _generate_m11_objective_endpoints_entry(self, doc, objective, endpoints):
    with doc.tag('tr'):
      with doc.tag('td', style="vertical-align: top; text-align: left"):
        with doc.tag('p'):
          doc.asis(objective)  
      with doc.tag('td', style="vertical-align: top; text-align: left"):
        for endpoint in endpoints:
          with doc.tag('p'):
            doc.asis(endpoint)

  def _generate_m11_title_page_entry(self, doc, title, entry):
    with doc.tag('tr'):
      with doc.tag('th', style="vertical-align: top; text-align: left"):
        with doc.tag('p'):
          doc.asis(title)  
      with doc.tag('td', style="vertical-align: top; text-align: left"):
        with doc.tag('p'):
          doc.asis(entry)
    # with doc.tag('tr', bgcolor="#F2F4F4"):
    #   with doc.tag('td', colspan="2", style="vertical-align: top; text-align: left; font-size: 12px"):
    #     #with doc.tag('span', style="vertical-align: top; text-align: left; font-size: 12px"):
    #     with doc.tag('i'):
    #       with doc.tag('span', style="color: #2AAA8A"):
    #         m11_reference = "Not set" if not m11_reference else m11_reference
    #         doc.text(f"M11: {m11_reference}")  
    #       with doc.tag('br'):
    #         pass
    #       with doc.tag('span', style="color: #FA8072"):
    #         doc.text(f"USDM: {', '.join(self._list_references(entry))}")  

  def _sponsor_identifier(self):
    identifiers = self.study_version.studyIdentifiers
    for identifier in identifiers:
      if identifier.studyIdentifierScope.type.code == 'C70793':
        return identifier
    return None
  
  def _study_phase(self):
    phase = self.study_version.studyPhase.standardCode
    results = [{'instance': phase, 'klass': 'Code', 'attribute': 'decode'}]
    return self._set_of_references(results)
  
  def _study_short_title(self):
    results = [{'instance': self.protocol_document_version, 'klass': 'StudyProtocolDocumentVersion', 'attribute': 'briefTitle'}]
    return self._set_of_references(results)

  def _study_full_title(self):
    results = [{'instance': self.protocol_document_version, 'klass': 'StudyProtocolDocumentVersion', 'attribute': 'officialTitle'}]
    return self._set_of_references(results)

  def _study_acronym(self):
    results = [{'instance': self.study_version, 'klass': 'StudyVersion', 'attribute': 'studyAcronym'}]
    return self._set_of_references(results)

  def _study_version(self):
    results = [{'instance': self.study_version, 'klass': 'StudyVersion', 'attribute': 'versionIdentifier'}]
    return self._set_of_references(results)

  def _study_identifier(self):
    identifier = self._sponsor_identifier()
    results = [{'instance': identifier, 'klass': 'StudyIdentifier', 'attribute': 'studyIdentifier'}]
    return self._set_of_references(results)

  def _study_regulatory_identifiers(self):
    results = []
    identifiers = self.study_version.studyIdentifiers
    for identifier in identifiers:
      if identifier.studyIdentifierScope.type.code == 'C188863' or identifier.studyIdentifierScope.type.code == 'C93453':
        item = {'instance': identifier, 'klass': 'StudyIdentifier', 'attribute': 'studyIdentifier'}
        results.append(item)
    return self._set_of_references(results)

  def _study_date(self):
    dates = self.protocol_document_version.dateValues
    for date in dates:
      if date.type.code == 'C99903x1':
        results = [{'instance': date, 'klass': 'GovernanceDate', 'attribute': 'dateValue'}]
        return self._set_of_references(results)
    return None
  
  def _approval_date(self):
    dates = self.study_version.dateValues
    for date in dates:
      if date.type.code == 'C132352':
        results = [{'instance': date, 'klass': 'GovernanceDate', 'attribute': 'dateValue'}]
        return self._set_of_references(results)
    return None

  def _organization_name_and_address(self):
    identifier = self._sponsor_identifier()
    results = [
      {'instance': identifier.studyIdentifierScope, 'klass': 'Organization', 'attribute': 'name'},
      {'instance': identifier.studyIdentifierScope.legalAddress, 'klass': 'Address', 'attribute': 'text'},
    ]
    return self._set_of_references(results)

  def _amendment(self):
    amendments = self.study_version.amendments
    results = [{'instance': amendments[-1], 'klass': 'StudyAmendment', 'attribute': 'number'}]
    return self._set_of_references(results)

  def _amendment_scopes(self):
    results = []
    amendment = self.study_version.amendments[-1]
    for item in amendment.enrollments:
      if item.type.code == "C68846":
        results = [{'instance': item.type, 'klass': 'Code', 'attribute': 'decode'}]
        return self._set_of_references(results)
      else:
        entry = {'instance': item.code.standardCode, 'klass': 'Code', 'attribute': 'decode'}
        results.append(entry)
    return self._set_of_references(results)
  
  def _criteria(self, type):
    results = []
    items = [c for c in self.study_design.population.criteria if c.category.code == type ]
    items.sort(key=lambda d: d.identifier)
    for item in items:
      result = {'identifier': item.identifier, 'text': item.text}
      dictionary = cross_references.get_by_id('SyntaxTemplateDictionary', item.dictionaryId)
      if dictionary:
        result['text'] = self._substitute_tags(result['text'], dictionary)
      results.append(result)
    return results

  def _objective_endpoints(self):
    results = []
    for item in self.study_design.objectives:
      result = {'objective': item.text, 'endpoints': []}
      dictionary = cross_references.get_by_id('SyntaxTemplateDictionary', item.dictionaryId)
      if dictionary:
        result['objective'] = self._substitute_tags(result['objective'], dictionary)
      for endpoint in item.endpoints:
        dictionary = cross_references.get_by_id('SyntaxTemplateDictionary', endpoint.dictionaryId)
        ep_text = endpoint.text
        if dictionary:
          ep_text = self._substitute_tags(ep_text, dictionary)
        result['endpoints'].append(ep_text)
      results.append(result)
    return results

  def _substitute_tags(self, text, dictionary):
      tags = re.findall(r'\[([^]]*)\]', text)
      for tag in tags:
        if tag in dictionary.parameterMap:
          map = dictionary.parameterMap[tag]
          text = text.replace(f"[{tag}]", f'<usdm:ref klass="{map["klass"]}" id="{map["id"]}" attribute="{map["attribute"]}"/>')
      return text

  # def _list_references(self, content_text):
  #   references = []
  #   soup = BeautifulSoup(content_text, 'html.parser')
  #   for ref in soup(['usdm:ref']):
  #     attributes = ref.attrs
  #     if 'path' in attributes:
  #       path = f"{attributes['path']}"
  #     else:
  #       path = f"{attributes['klass']}/@{attributes['attribute']}"
  #     if path not in references:
  #       references.append(path)
  #   return references if references else ['No mapping path']
  
  def _set_of_references(self, items):
    if items:
      return ", ".join([f'<usdm:ref klass="{item["klass"]}" id="{item["instance"].id}" attribute="{item["attribute"]}" path="{item["path"]}"/>' for item in items])
    else:
      return ""

  def _front_sheet(title):
    return f"""
      <div id="title-page" class="page">
        <h1>{title}</h1>
        <div id="header-and-footer">
          <span id="page-number"></span>
        </div>
      </div>
    """
    #   <div id="toc-page" class="page">
    #     <div id="table-of-contents">
    #       {''.join(chapters)}
    #     </div>
    #     <div id="header-and-footer">
    #       <span id="page-number"></span>
    #     </div>
    #   </div>
    # """