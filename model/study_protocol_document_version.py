import re
import traceback
import yaml
import logging
from yattag import Doc
from bs4 import BeautifulSoup   
from typing import List
from .node import *
from .code import Code
from .governance_date import GovernanceDate
from .narrative_content import NarrativeContent
from uuid import uuid4

class StudyProtocolDocumentVersion(NodeId):
  briefTitle: str
  officialTitle: str
  publicTitle: str
  scientificTitle: str
  protocolVersion: str
  protocolStatus: Code = None
  dateValues: List[GovernanceDate] = []
  contents: List[NarrativeContent] = []
  childrenIds: List[str] = []

  def document(self):
    try:
      doc = Doc()
      with doc.tag('body'):
        #doc.asis(front_sheet)    
        items = self._narrative_content()
        for section in self.section_list_flat():
          try:
            content = items[section['key']]
          except Exception as e:
            logging.warning(f"Protocol document section {section['key']} not found")
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
      logging.error(f"Exception raised while building protocol document")
      logging.error(f"Exception {e}\n{traceback.format_exc()}")
      return {'error': f"Exception. Failed to build protocol document"}

  def section(self, key):
    return self._read_section_definition(key)

  def section_list_flat(self):
    items = self._read_section_list()
    results = []
    for item in items:
      results.append(item)
      if not item['items'] == []:
        results += item['items']
    return results

  def section_list(self):
    return {'root': self._read_section_list()}

  def _read_section_definition(self, key):
    data = self._read_as_yaml_file("data/m11_to_usdm.yaml")
    return data[key]
  
  def _read_section_list(self):
    return self._read_as_yaml_file("data/m11_sections.yaml")
  
  def _read_as_yaml_file(self, filepath):
    with open(filepath, "r") as f:
      data = yaml.load(f, Loader=yaml.FullLoader)
    return data
  
  def _narrative_content(self):
    db = Neo4jConnection()
    with db.session() as session:
      return session.execute_read(self._narrative_content_read, self.uuid)

  @staticmethod
  def _narrative_content_read(tx, uuid):
    query = """
      MATCH (spdv:StudyProtocolDocumentVersion {uuid: $uuid1})-[:CONTENTS_REL]->(nc:NarrativeContent) RETURN nc
    """
    rows = tx.run(query, uuid1=uuid)
    results = {}
    for item in rows:
      #print(f"ITEM: {item}")
      nc = NarrativeContent.wrap(item['nc'])
      results[nc.sectionNumber] = nc
    #print(f"RESULTS: {results}")
    return results
  
  def _content_to_html(self, content, doc):
    level = content.level()
    klass = "page" if level == 1 else ""
    heading_id = f"section-{content.sectionNumber}"
    with doc.tag('div', klass=klass):
      if (level == 1 and int(content.sectionNumber) > 0) or (level > 1):
        with doc.tag(f'h{level + 4}', id=heading_id):
          doc.asis(f"{content.sectionNumber}&nbsp{content.sectionTitle}")
      if self._standard_section(content.text):
        name = self._standard_section_name(content.text)
        content.text = self._generate_standard_section(name)
      doc.asis(self._translate_references(content.text))

  def _translate_references(self, content_text):
    return content_text
    # soup = BeautifulSoup(content_text, 'html.parser')
    # for ref in soup(['usdm:ref']):
    #   attributes = ref.attrs
    #   if 'namexref' in attributes:
    #     instance = cross_references.get(attributes['klass'], attributes['namexref'])
    #   else:
    #     instance = cross_references.get_by_id(attributes['klass'], attributes['id'])
    #   try:
    #     value = str(getattr(instance, attributes['attribute']))
    #     translated_text = self._translate_references(value)
    #     ref.replace_with(translated_text)
    #   except Exception as e:
    #     logging.error(f"Failed to translate reference, attributes {attributes}\n{traceback.format_exc()}")
    #     error_manager.add(None, None, None, f"Failed to translate a reference {attributes} while generating the HTML document")
    #     ref.replace_with('Missing content')
    # return str(soup)
  
  def _standard_section(self, text):
    return False
    # soup = BeautifulSoup(text, 'html.parser')
    # for section in soup(['usdm:section']):
    #   return True
    # return False
  
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
    #print(f"M11 TP:")
    doc = Doc()
    with doc.tag('table'):
      self._generate_m11_title_page_entry(doc, 'Sponsor Confidentiality Statement:', '', 'Enter Sponsor Confidentiality Statement')
      self._generate_m11_title_page_entry(doc, 'Full Title:', f'{self._study_full_title()}', 'Enter Full Title')
      self._generate_m11_title_page_entry(doc, 'Trial Acronym:', f'{self._study_acronym()}', 'Enter trial Acronym')
      self._generate_m11_title_page_entry(doc, 'Protocol Identifier:', f'{self._study_identifier()}', 'Enter Protocol Identifier')
      self._generate_m11_title_page_entry(doc, 'Original Protocol:', '', 'Original protocol')
      self._generate_m11_title_page_entry(doc, 'Version Number:', f'{self._study_version()}', 'Enter Version Number')
      self._generate_m11_title_page_entry(doc, 'Version Date:', f'{self._study_date()}', 'Enter Version Date')
      self._generate_m11_title_page_entry(doc, 'Amendment Identifier:', f'{self._amendment()}', 'Amendment Identifier')
      self._generate_m11_title_page_entry(doc, 'Amendment Scope:', f'{self._amendment_scopes()}', 'Amendment Scope')
      self._generate_m11_title_page_entry(doc, 'Compound Codes(s):', '', 'Enter Compound Code(s)')
      self._generate_m11_title_page_entry(doc, 'Compound Name(s):', '', 'Enter Nonproprietary Name(s), Enter Proprietary Name(s)')
      self._generate_m11_title_page_entry(doc, 'Trial Phase:', f'{self._study_phase()}', 'Trial Phase')
      self._generate_m11_title_page_entry(doc, 'Short Title:', f'{self._study_short_title()}', 'Enter Trial Short Title')
      self._generate_m11_title_page_entry(doc, 'Sponsor Name and Address:', f'{self._organization_name_and_address()}', 'Enter Sponsor Name, Enter Sponsor Legal Address')
      self._generate_m11_title_page_entry(doc, 'Regulatory Agency Identifier Number(s):', f'{self._study_regulatory_identifiers()}', 'EU CT Number, IDE Number, FDA IND Number, JRCT Number, NCT Number, NMPA IND Number, WHO/UTN Number, Other Regulatory Agency Identifier Number')
      self._generate_m11_title_page_entry(doc, 'Spondor Approval Date:', f'{self._approval_date()}', 'Enter Approval Date or state location where information can be found')

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

  def _generate_m11_title_page_entry(self, doc, title, entry, m11_reference=''):
    with doc.tag('tr'):
      with doc.tag('th', style="vertical-align: top; text-align: left"):
        with doc.tag('p'):
          doc.asis(title)  
      with doc.tag('td', style="vertical-align: top; text-align: left"):
        with doc.tag('p'):
          doc.asis(entry)
    with doc.tag('tr', bgcolor="#F2F4F4"):
      with doc.tag('td', colspan="2", style="vertical-align: top; text-align: left; font-size: 12px"):
        #with doc.tag('span', style="vertical-align: top; text-align: left; font-size: 12px"):
        with doc.tag('i'):
          with doc.tag('span', style="color: #2AAA8A"):
            m11_reference = "Not set" if not m11_reference else m11_reference
            doc.text(f"M11: {m11_reference}")  
          with doc.tag('br'):
            pass
          with doc.tag('span', style="color: #FA8072"):
            doc.text(f"USDM: {', '.join(self._list_references(entry))}")  

  def _sponsor_identifier(self):
    identifiers = self.study_version.studyIdentifiers
    for identifier in identifiers:
      if identifier.studyIdentifierScope.type.code == 'C70793':
        return identifier
    return None
  
  def _study_phase(self):
    phase = self.study_version.studyPhase.standardCode
    results = [{'instance': phase, 'klass': 'Code', 'attribute': 'decode', 'path': 'StudyVersion/@studyPhase/@standardCode/@decode'}]
    return self._set_of_references(results)
  
  def _study_short_title(self):
    results = [{'instance': self.protocol_document_version, 'klass': 'StudyProtocolDocumentVersion', 'attribute': 'briefTitle', 'path': 'StudyProtocolDocumentVersion/@briefTitle'}]
    return self._set_of_references(results)

  def _study_full_title(self):
    results = [{'instance': self.protocol_document_version, 'klass': 'StudyProtocolDocumentVersion', 'attribute': 'officialTitle', 'path': 'StudyProtocolDocumentVersion/@officialTitle'}]
    return self._set_of_references(results)

  def _study_acronym(self):
    results = [{'instance': self.study_version, 'klass': 'StudyVersion', 'attribute': 'studyAcronym', 'path': 'StudyVersion/@studyAcronym'}]
    return self._set_of_references(results)

  def _study_version(self):
    results = [{'instance': self.study_version, 'klass': 'StudyVersion', 'attribute': 'versionIdentifier', 'path': 'StudyVersion/@versionIdentifier'}]
    return self._set_of_references(results)

  def _study_identifier(self):
    identifier = self._sponsor_identifier()
    results = [{'instance': identifier, 'klass': 'StudyIdentifier', 'attribute': 'studyIdentifier', 'path': 'StudyIdentifier[Organization/@type/@code=C70793]/@studyIdentifier'}]
    return self._set_of_references(results)

  def _study_regulatory_identifiers(self):
    results = []
    identifiers = self.study_version.studyIdentifiers
    for identifier in identifiers:
      if identifier.studyIdentifierScope.type.code == 'C188863' or identifier.studyIdentifierScope.type.code == 'C93453':
        item = {'instance': identifier, 'klass': 'StudyIdentifier', 'attribute': 'studyIdentifier', 'path': 'StudyIdentifier[Organization/@type/@code=C188863|C93453]/@studyIdentifier'}
        results.append(item)
    return self._set_of_references(results)

  def _study_date(self):
    dates = self.protocol_document_version.dateValues
    for date in dates:
      if date.type.code == 'C99903x1':
        results = [{'instance': date, 'klass': 'GovernanceDate', 'attribute': 'dateValue', 'path': 'StudyProtocolDocumentVersion/GovernanceDate[@type/@code=C99903x1]/@dateValue'}]
        return self._set_of_references(results)
    return None
  
  def _approval_date(self):
    dates = self.study_version.dateValues
    for date in dates:
      if date.type.code == 'C132352':
        results = [{'instance': date, 'klass': 'GovernanceDate', 'attribute': 'dateValue', 'path': 'StudyVersion/GovernanceDate[@type/@code=C132352]/@dateValue'}]
        return self._set_of_references(results)
    return None

  def _organization_name_and_address(self):
    identifier = self._sponsor_identifier()
    results = [
      {'instance': identifier.studyIdentifierScope, 'klass': 'Organization', 'attribute': 'name', 'path': 'StudyIdentifier[Organization/@type/@code=C70793]/Organization/@name'},
      {'instance': identifier.studyIdentifierScope.legalAddress, 'klass': 'Address', 'attribute': 'text', 'path': 'StudyIdentifier[Organization/@type/@code=C70793]/Organization/Address/@text'},
    ]
    return self._set_of_references(results)

  def _amendment(self):
    amendments = self.study_version.amendments
    results = [{'instance': amendments[-1], 'klass': 'StudyAmendment', 'attribute': 'number', 'path': 'StudyVersion/StudyAmendment/@number'}]
    return self._set_of_references(results)

  def _amendment_scopes(self):
    results = []
    amendment = self.study_version.amendments[-1]
    for item in amendment.enrollments:
      if item.type.code == "C68846":
        results = [{'instance': item.type, 'klass': 'Code', 'attribute': 'decode', 'path': 'StudyVersion/StudyAmendment/SubjectEnrollment[@type/@code=C68846]/Code/@decode'}]
        return self._set_of_references(results)
      else:
        entry = {'instance': item.code.standardCode, 'klass': 'Code', 'attribute': 'decode', 'path': 'StudyVersion/StudyAmendment/SubjectEnrollment/@code/@standardCode/@decode'}
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

  def _list_references(self, content_text):
    references = []
    soup = BeautifulSoup(content_text, 'html.parser')
    for ref in soup(['usdm:ref']):
      attributes = ref.attrs
      if 'path' in attributes:
        path = f"{attributes['path']}"
      else:
        path = f"{attributes['klass']}/@{attributes['attribute']}"
      if path not in references:
        references.append(path)
    return references if references else ['No mapping path']
  
  def _set_of_references(self, items):
    if items:
      return ", ".join([f'<usdm:ref klass="{item["klass"]}" id="{item["instance"].id}" attribute="{item["attribute"]}" path="{item["path"]}"/>' for item in items])
    else:
      return ""
