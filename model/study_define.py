# from typing import List, Union
from uuid import uuid4
from d4kms_service import Neo4jConnection
# from d4kms_generic import application_logger
# from model.domain import Domain
# from model.variable import Variable
# from service.bc_service import BCService
# from service.sdtm_service import SDTMService
# from model.crm import CRMNode
# from model.biomedical_concept import BiomedicalConceptSimple
from service.ct_service import CTService
from datetime import datetime as dt

# import json
# import copy
import traceback

from model.utility.define_queries import define_vlm_query, study_info_query, domains_query, domain_variables_query, variables_crm_link_query, define_codelist_query, define_test_codes_query, find_ct_query
from datetime import datetime
import xml.etree.ElementTree as ET
from pathlib import Path


class StudyDefine():
  define_xml: str

  @classmethod
  def make_define(cls, uuid, page, size, filter):
    xml = main(uuid)
    result = {'xml': xml, 'items': [], 'page': page, 'size': size, 'filter': filter, 'count': 1 }
    return result


DATATYPES = {
   'coding': 'string',
   'quantity': 'float',
   'Char': 'text',
   'Num': 'integer',
}


# ISSUE: Should be in DB. Could add to configuration
ORDER_OF_DOMAINS = [
  'TRIAL DESIGN',
  'SPECIAL PURPOSE',
  'INTERVENTIONS',
  'EVENTS',
  'FINDINGS',
  'FINDINGS ABOUT',
  'RELATIONSHIP',
  'STUDY REFERENCE',
]

# All possible classes
# ['ADAM OTHER', 'BASIC DATA STRUCTURE', 'DEVICE LEVEL ANALYSIS DATASET', 'EVENTS', 'FINDINGS', 'FINDINGS ABOUT', 'INTERVENTIONS', 'MEDICAL DEVICE BASIC DATA STRUCTURE', 'MEDICAL DEVICE OCCURRENCE DATA STRUCTURE', 'OCCURRENCE DATA STRUCTURE', 'REFERENCE DATA STRUCTURE', 'RELATIONSHIP', 'SPECIAL PURPOSE', 'STUDY REFERENCE', 'SUBJECT LEVEL ANALYSIS DATASET', 'TRIAL DESIGN']
# ISSUE: Should be in DB
# ISSUE: 'SPECIAL-PURPOSE' -> 'SPECIAL PURPOSE'
DOMAIN_CLASS = {
  'EVENTS'          :['AE', 'BE', 'CE', 'DS', 'DV', 'HO', 'MH'],
  'FINDINGS'        :['BS', 'CP', 'CV', 'DA', 'DD', 'EG', 'FT', 'GF', 'IE', 'IS', 'LB', 'MB', 'MI', 'MK', 'MS', 'NV', 'OE', 'PC', 'PE', 'PP', 'QS', 'RE', 'RP', 'RS', 'SC', 'SS', 'TR', 'TU', 'UR', 'VS'],
  'FINDINGS ABOUT'  :['FA', 'SR'],
  'INTERVENTIONS'   :['AG', 'CM', 'EC', 'EX', 'ML', 'PR', 'SU'],
  'RELATIONSHIP'    :['RELREC', 'RELSPEC', 'RELSUB', 'SUPPQUAL'],
  'SPECIAL PURPOSE' :['CO', 'DM', 'SE', 'SM', 'SV'],
  'STUDY REFERENCE' :['OI'],
  'TRIAL DESIGN'    :['TA', 'TD', 'TE', 'TI', 'TM', 'TS', 'TV'],
}

DOMAIN_KEY_SEQUENCE = {
  'AE' : {'STUDYID': '1', 'USUBJID': '2', 'AEDECOD': '3', 'AESTDTC': '4'},
  'DM' : {'STUDYID': '1', 'USUBJID': '2'},
  'DS' : {'STUDYID': '1', 'USUBJID': '2', 'DSDECOD': '3', 'DSSTDTC': '4'},
  'EX' : {'STUDYID': '1', 'USUBJID': '2', 'EXTRT': '3', 'EXSTDTC': '4'},
#  'LB' : {'STUDYID': '1', 'USUBJID': '2', 'LBTESTCD': '3', 'LBSPEC': '4', 'VISITNUM': '5', 'LBTPTREF': '6', 'LBTPTNUM': '7'},
  'LB' : {'STUDYID': '1', 'USUBJID': '2', 'LBTESTCD': '3', 'LBSPEC': '4', 'VISITNUM': '5', 'LBDTC': '6'},
  'VS' : {'STUDYID': '1', 'USUBJID': '2', 'VSTESTCD': '3', 'VSSPEC': '4', 'VISITNUM': '5', 'VSTPTREF': '6', 'VSTPTNUM': '7'}
}

ct = CTService()
define_terms = {}

xml_header = """<?xml version="1.0" encoding="UTF-8"?>\n<?xml-stylesheet type="text/xsl" href="stylesheets/define2-1.xsl"?>\n"""

def get_study_info(uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = study_info_query(uuid)
      # print('study_info query'); print(query)
      results = session.run(query)
      data = [r.data() for r in results]
    db.close()
    return data[0]

def get_domains(uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = domains_query(uuid)
      # print("domains query", query)
      results = session.run(query)
      data = [r['d'] for r in results]
    db.close()
    return data

def get_variables(uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = domain_variables_query(uuid)
      # print("variables query", query)
      results = session.run(query)
      # all_variables = [r['v'] for r in results]
      all_variables = [r['v'] for r in results.data()]
      required_variables = [v for v in all_variables if v['core'] == 'Req']

      # CRM linked vars
      query = variables_crm_link_query(uuid)
      results = session.run(query)
      vars_in_use = [r['v'] for r in results.data()]
    db.close()
    # return vars_in_use
    return all_variables


def get_define_vlm(domain_uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = define_vlm_query(domain_uuid)
      # print("vlm query"); print(query)
      results = session.run(query)
      data = [r for r in results.data()]
    db.close()
    # for k in data:
    #    print("def vlm",k)
    return data

def get_define_codelist(domain_uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = define_codelist_query(domain_uuid)
      # print("codelist query"); print(query)
      results = session.run(query)
      data = [r for r in results.data()]
    db.close()
    # for k in data:
    #    print("def codeslist",k)
    return data

def get_concept_info(items):
    data = []
    for item in items:
      if item['code'] in define_terms:
          data.append(define_terms[item['code']])
      else:
        if 'notation' in item and item['notation'] != None:
          # print("not using ct.find :)")
          cli = {'code': item['code'], 'pref_label': item['pref_label'], 'notation': item['notation']}        
          # print("cli", cli)
        else:
          print("using ct.find :/")
          response = ct.find_by_identifier(item['code'])
          first = response[0] if len(response) > 0 else None
          if first:
            cli = {'code': first['child']['identifier'], 'pref_label': first['child']['pref_label'], 'notation': first['child']['notation']}
        data.append(cli)
        define_terms[item['code']] = cli
    return data

def get_define_test_codes(domain_uuid):
    db = Neo4jConnection()
    with db.session() as session:
      query = define_test_codes_query(domain_uuid)
      # print("test_codes query"); print(query)
      results = session.run(query)
      data = [r for r in results.data()]
    db.close()
    return data

def pretty_string(text):
   return text.replace(' ','_')

def odm_properties(root):
  now = datetime.now().isoformat()
  root.set('xmlns',"http://www.cdisc.org/ns/odm/v1.3")
  root.set('xmlns:xlink',"http://www.w3.org/1999/xlink")
  root.set('xmlns:def',"http://www.cdisc.org/ns/def/v2.1")
  root.set('ODMVersion',"1.3.2")
  root.set('FileOID',"www.cdisc.org/StudyCDISC01_1/1/Define-XML_2.1.0")
  root.set('FileType',"Snapshot")
  root.set('CreationDateTime',now)
  root.set('Originator',"Study Service")
  root.set('SourceSystem',"Study service")
  root.set('SourceSystemVersion',"Alpha1")
  root.set('def:Context',"Other")

def set_study_info(oid= 'tbd', study_name = 'tbd', description = 'tbd', protocol_name = 'tbd'):
  study = ET.Element('Study')
  study.set('OID',study_name)
  # study.set('StudyDescription', description)
  # study.set('ProtocolName', protocol_name)
  return study

# ISSUE: Hardcoded
def set_globalvariables(study_name = None, study_description = "Not set", protocol_name = "Not set"):
  global_variables = ET.Element('GlobalVariables')
  element = ET.Element('StudyName')
  element.text = study_name
  global_variables.append(element)
  element = ET.Element('StudyDescription')
  element.text = study_description
  global_variables.append(element)
  element = ET.Element('ProtocolName')
  element.text = protocol_name
  global_variables.append(element)
  return global_variables

def comment_def(oid, text, lang = 'en', leaf_id = None, page_refs = None, ref_type = None):
  c = ET.Element('def:CommentDef')
  c.set('OID', oid)
  d = ET.SubElement(c,'Description')
  d.append(translated_text(text, lang))
  # Add page reference
  return c
   
# NOTE: Hardcoded
def comment_defs():
  comment_defs = []
  comment_def_oid = "COM.STD.1"
  comment = comment_def(comment_def_oid, "From CDISC Library SDTM")
  comment_defs.append(comment)
  comment_def_oid = "COM.STD.2"
  comment = comment_def(comment_def_oid, "From CDISC Library Controlled Terminology")
  comment_defs.append(comment)
  return comment_defs

# NOTE: Hardcoded
def standards():
  standards = ET.Element('def:Standards')

  standard1 = ET.Element('def:Standard')
  standard1.set("OID", "STD.1")
  standard1.set("Name", "SDTMIG")
  standard1.set("Type", "IG")
  standard1.set("Version", "3.4")
  standard1.set("Status", "Final")
  standard1.set("def:CommentOID", "COM.STD.1")
  standards.append(standard1)

  standard1 = ET.Element('def:Standard')
  standard1.set("OID", "STD.2")
  standard1.set("Name", "CDISC/NCI")
  standard1.set("Type", "CT")
  standard1.set("Version", "2023-12-09")
  standard1.set("Status", "Final")
  standard1.set("def:CommentOID", "COM.STD.2")
  standards.append(standard1)

  return standards

def metadata_version(oid = 'Not set', name = 'Not set', description = None):
  metadata = ET.Element('MetaDataVersion')
  metadata.set("OID", oid)
  metadata.set("Name", name)
  if description:
    metadata.set("Description", description)
  else:
    metadata.set("Description", "Generated from USDM and Biomedical Concepts (CDISC Library + d4k adaptions)")
  metadata.set("def:DefineVersion", "2.1.7")
  return metadata

# def get_unique_vars(vars):
#   unique_vars = []
#   for v in vars:
#       if 'bc' in v:
#         v.pop('bc')
#       if 'bc_uuid' in v:
#         v.pop('bc_uuid')
#       if 'decodes' in v:
#         v.pop('decodes')
#       unique_vars.append(v)
#   unique_vars = list({v['var_uuid']:v for v in unique_vars}.values())
#   return unique_vars


def get_domains_and_variables(uuid):
  domains = []
  raw_domains = get_domains(uuid)
  for d in raw_domains:
    item = {}
    for k,v in d._properties.items():
        item[k] = v
    all_variables = get_variables(d['uuid'])
    for v in all_variables:
      v['domain'] = d['name']
    codelist_metadata = get_define_codelist(d['uuid'])
    item['codelist'] = codelist_metadata
    if d['goc'] in ['FINDINGS','FINDINGS ABOUT']:
      test_codes = get_define_test_codes(d['uuid'])
      item['test_codes'] = test_codes
    vlm_metadata = get_define_vlm(d['uuid'])
    item['vlm'] = vlm_metadata
    item['goc'] = next((x for x,y in DOMAIN_CLASS.items() if d['name'] in y), "Fix")
    item['variables'] = all_variables
    domains.append(item)

  return domains

def translated_text(text, language = 'en'):
    translated_text = ET.Element('TranslatedText')
    translated_text.set('xml:lang',language)
    translated_text.text = text
    return translated_text

def description(language, text_str):
    description = ET.Element('Description')
    description.append(translated_text(text_str, language))
    return description

def origin(type, source):
    origin = ET.Element('def:Origin')
    origin.set('Type', type)
    origin.set('Source', source)
    return origin

def leaf(id, href, text_str):
    leaf = ET.Element('def:leaf')
    leaf.set("ID", id)
    leaf.set("xlink:href", href)
    title = ET.SubElement(leaf, 'def:title')
    title.text = text_str
    return leaf

def set_variable_refs(variables):
    variable_refs = []
    for v in variables:
      ref = ET.Element('ItemRef')
      # print(f"  variable_refs item_oid {item_def_oid(v)}")
      ref.set('ItemOID', item_def_oid(v))
      mandatory = 'No' if v['core'] == 'Perm' else 'Yes'
      ref.set('Mandatory', mandatory)
      order = int(v['ordinal'])
      ref.set('OrderNumber', str(order))
      if v['name'] in DOMAIN_KEY_SEQUENCE[v['domain']]:
        ref.set('KeySequence', DOMAIN_KEY_SEQUENCE[v['domain']][v['name']])
      variable_refs.append(ref)
    return variable_refs

def item_group_defs(domains):
    igds = []
    for d in domains:
        igd = ET.Element('ItemGroupDef')
        igd.set('OID', d['uuid'])
        igd.set('Domain', d['name'])
        igd.set('Name', d['name'])
        igd.set('Repeating', 'No')
        igd.set('IsReferenceData', 'No')
        igd.set('SASDatasetName', d['name'])
        igd.set('def:Structure', d['structure'])
        igd.set('Purpose', 'Tabulation')
        igd.set('Keys', d['ordinal'])
        igd.set('def:StandardOID', 'STD.1')
        igd.set('def:ArchiveLocationID', f"LI.{d['name']}")
        igd.append(description('en',d['label']))
        # NOTE: ISSUE/Question: Why does the order matter? Had to move refs after description
        refs = set_variable_refs(d['variables'])
        for ref in refs:
          igd.append(ref)
        ET.SubElement(igd,'def:Class', {'Name': d['goc']})
        # ET.SubElement(igd,'def:Class').text = goc
        # goc_e.text = goc
        igd.append(leaf(f"LI.{d['name']}", d['name'].lower()+".xpt", d['name'].lower()+".xpt"))
        igds.append(igd)
    return igds

def item_def_oid(item):
    # return f"IT.{pretty_string(item['name'])}.{item['uuid']}"
    return f"IT.{item['domain']}.{pretty_string(item['name'])}"

def item_def_vlm_oid(item):
    # return f"ITC.{pretty_string(item['name'])}.{item['testcd']}.{item['uuid']}"
    return f"IDVO.{item['domain']}.{pretty_string(item['name'])}.{item['testcd']}"

def item_defs_variable(domains):
    idfs = []
    for d in domains:
        for item in d['variables']:
          idf = ET.Element('ItemDef')
          idf.set('OID', item_def_oid(item))
          idf.set('Name', item['name'])
          datatype = DATATYPES[item['datatype']] if 'datatype' in item else ""
          if datatype == "":
            # NOTE: Using SDTM datatype. Not always correct e.g. VISITNUM
            datatype = DATATYPES[item['data_type']]
          if datatype == "":
             print("-- item_defs_variable CHECK", item['name'])
          idf.set('DataType', datatype)
          idf.set('Length', '8')
          idf.set('SASFieldName', item['name'])
          idf.append(description('en',item['label']))
          if d['goc'] in ['FINDINGS','FINDINGS ABOUT']:
            # If variable has vlm
            if next((x for x in d['vlm'] if x['var_uuid'] == item['uuid']), None):
              vl_ref = ET.Element('def:ValueListRef')
              vl_ref.set('ValueListOID', value_list_oid(item))
              idf.append(vl_ref)
            # If variable only has codelist
            elif next((x for x in d['codelist'] if x['uuid'] == item['uuid']), None):
              cl_ref = ET.Element('CodeListRef')
              cl_ref.set('CodeListOID', codelist_oid(item))
              idf.append(cl_ref)
          else:
            # If variable only has codelist
            if next((x for x in d['codelist'] if x['uuid'] == item['uuid']), None):
              cl_ref = ET.Element('CodeListRef')
              cl_ref.set('CodeListOID', codelist_oid(item))
              idf.append(cl_ref)

          idf.append(origin('Collected','Sponsor'))
          idfs.append(idf)
    return idfs

def var_test_key(item):
   return f"{item['name']}.{item['testcd']}"

def vlm_item_defs(domains):
    idfs = {}
    for d in domains:
      if d['goc'] in ['FINDINGS','FINDINGS ABOUT']:
        for item in d['vlm']:
          key = var_test_key(item)
          if key in idfs:
            pass
          else:
            idf = ET.Element('ItemDef')
            idf.set('OID', item_def_vlm_oid(item))
            idf.set('Name', f"{item['name']} {item['testcd']}")
            datatype = DATATYPES[item['datatype']] if 'datatype' in item else ""
            if datatype == "":
              # NOTE: Using SDTM datatype. Not always correct e.g. VISITNUM
              datatype = DATATYPES[item['data_type']]
            if datatype == "":
              print("-- vlm_item_defs CHECK", item['name'])
            idf.set('DataType', datatype)
            idf.set('Length', '8')
            idf.set('SASFieldName', item['name'])
            idf.append(description('en',item['label']))
            # vlm item always has a codelist
            cl_ref = ET.Element('CodeListRef')
            cl_ref.set('CodeListOID', vlm_codelist_oid(item))
            idf.append(cl_ref)
            idf.append(origin('Collected','Sponsor'))

            idfs[key] = idf
    return list(idfs.values())

def codelist_oid(item):
    # return f"CL.{pretty_string(variable)}.{uuid}"
    # return f"CL.{pretty_string(item['name'])}.{item['uuid']}"
    return f"CL.{pretty_string(item['name'])}"

def test_codelist_oid(item):
    # return f"CL.{item['domain']}.{pretty_string(variable)}"
    return f"TEST.CL.{item['domain']}.{item['domain']}TESTCD"
    # return f"CL.{variable}"

def vlm_codelist_oid(item):
    # return f"CL.{item['domain']}.{pretty_string(variable)}"
    return f"VLM.CL.{item['domain']}.{pretty_string(item['name'])}.{item['testcd']}"

def alias(context, code):
    a = ET.Element('Alias')
    a.set('Context', context)
    a.set('Name', code)
    return a

def enumerated_item(code, context, value):
    e = ET.Element('EnumeratedItem')
    e.set('CodedValue', value)
    e.append(alias(context, code))
    return e

def codelist_item(code, short, long, context):
    e = ET.Element('CodeListItem')
    e.set('CodedValue', short)
    d = ET.SubElement(e,'Decode')
    d.append(translated_text(long))
    if code:
      e.append(alias(context, code))
    return e

def codelist_name(item):
   if 'testcd' in item:
     return f"CL {item['name']} {item['testcd']} ({item['bc']})"
   else:
     return f"CL {item['name']}"

# Create codelists for domain variable
def codelist_defs(domains):
    codelists = []
    for d in domains:
        for item in d['codelist']:
          cl = ET.Element('CodeList')
          cl.set('OID', codelist_oid(item))
          cl.set('Name', codelist_name(item))
          cl.set('def:StandardOID', "STD.2")
          datatype = DATATYPES[item['datatype']] if 'datatype' in item else ""
          if datatype == "":
            # NOTE: Using SDTM datatype. Not always correct e.g. VISITNUM
            # datatype = DATATYPES[item['data_type']]
            datatype = "string"

          if datatype == "":
            print("-- codelist_defs CHECK", item['name'])
          cl.set('DataType', datatype)
          codes = [x['code'] for x in item['decodes']]
          # print("--- check decodes")
          # for c in item['decodes']:
          #    print("c", c)
          # print("--- check codes")
          # for c in codes:
          #    print("c", c)
          clis = get_concept_info(item['decodes'])
          for cli in clis:
            # NOTE: Need to care for enumerated item?
            # cl.append(enumerated_item(x['code'], "nci:ExtCodeID",x['decode']))
            cl.append(codelist_item(cli['code'], cli['notation'], cli['pref_label'], "nci:ExtCodeID"))
          codelists.append(cl)
    return codelists

def vlm_codelist_name(item):
  return f"CL {item['domain']} {item['name']} {item['testcd']}"

# Create codelist for VLM
def vlm_codelists_defs(domains):
    vlm_codelists = {}
    for d in domains:
      if d['goc'] in ['FINDINGS','FINDINGS ABOUT']:
        for item in d['vlm']:
          key = var_test_key(item)
          if key in vlm_codelists:
            pass
          else:
            cl = ET.Element('CodeList')
            cl.set('OID', vlm_codelist_oid(item))
            cl.set('Name', vlm_codelist_name(item))
            cl.set('def:StandardOID', "STD.2")
            datatype = DATATYPES[item['datatype']] if 'datatype' in item else ""
            if datatype == "":
              # NOTE: Using SDTM datatype. Not always correct e.g. VISITNUM
              datatype = DATATYPES[item['data_type']]
            cl.set('DataType', datatype)
            cl.set('Length', '8')
            # cl.set('SASFieldName', item['name'])
            # cl.append(description('en',item['label']))
            # cl.append(origin('Collected','Sponsor'))
            codes = [x['code'] for x in item['decodes']]
               
            clis = get_concept_info(item['decodes'])
            for cli in clis:
              # NOTE: Need to care for enumerated item?
              # cl.append(enumerated_item(x['code'], "nci:ExtCodeID",x['decode']))
              cl.append(codelist_item(cli['code'], cli['notation'], cli['pref_label'], "nci:ExtCodeID"))
            vlm_codelists[key] = cl
    return list(vlm_codelists.values())


def test_codelist_name(item):
   return f"CL {item['domain']} ({item['domain']+'TESTCD'})"

# Create codelist for TESTCD/TEST
def test_codes_defs(domains):
    test_codes = []
    for d in domains:
        if 'test_codes' in d:
          for item in d['test_codes']:
            cl = ET.Element('CodeList')
            cl.set('OID', test_codelist_oid(item))
            cl.set('Name', test_codelist_name(item))
            cl.set('def:StandardOID', "STD.1")
            cl.set('DataType', "text")
            for test in item['test_codes']:
              # cl.append(enumerated_item(x['code'], "nci:ExtCodeID",x['decode']))
              cl.append(codelist_item(test['code'], test['testcd'], test['test'], "nci:ExtCodeID"))
            test_codes.append(cl)
    return test_codes

def value_list_oid(item):
    return f"VL.{item['domain']}.{item['name']}"

def value_list_defs(domains):
    vlds = []
    for d in domains:
        if d['goc'] in ['FINDINGS','FINDINGS ABOUT']:
          for v in d['variables']:
            vlms  = [x for x in d['vlm'] if x['var_uuid'] == v['uuid']]
            if vlms:
              # NOTE: Make one per test code (VLM)
              vld = ET.Element('def:ValueListDef')
              vld.set('OID', value_list_oid(v))
              item_refs = {}
              i = 1
              for vlm in vlms:
                key = var_test_key(vlm)
                if key in item_refs:
                  pass
                else:
                  item_ref = ET.Element('ItemRef')
                  item_ref.set('ItemOID', item_def_vlm_oid(vlm))
                  item_ref.set('OrderNumber', str(i))
                  item_ref.set('Mandatory', 'No')
                  wcd = ET.Element("def:WhereClauseRef")
                  wcd.set('WhereClauseOID', where_clause_oid(vlm)) 
                  item_ref.append(wcd)
                  i += 1
                  item_refs[key] = item_ref
              for ref in item_refs.values():
                vld.append(ref)
              vlds.append(vld)
    return vlds

def where_clause_oid(item):
    return f"WC.{item['domain']}.{item['name']}.{item['testcd']}" #.{var_uuid}"

def range_check(decodes,comparator, soft_hard, item_oid):
    range_check = ET.Element('RangeCheck')
    range_check.set('Comparator', comparator)
    range_check.set('SoftHard', soft_hard)
    range_check.set('def:ItemOID', item_oid)
    if isinstance(decodes, list):
      for decode in decodes:
        check_value = ET.Element('CheckValue')
        check_value.text = decode['decode']
        range_check.append(check_value)
    else:
        check_value = ET.Element('CheckValue')
        check_value.text = decodes
        range_check.append(check_value)
    return range_check

def where_clause_defs(domains):
    wcds = {}
    for d in domains:
        if d['goc'] in ['FINDINGS','FINDINGS ABOUT']:
          testcd_var = next((v for v in d['variables'] if v['name'] == d['name']+"TESTCD"),"Not found")
          testcd_oid = item_def_oid(testcd_var)
          for v in d['vlm']:
            key = var_test_key(v)
            if key in wcds:
              pass
            else:
              wcd = ET.Element('def:WhereClauseDef')
              wcd.set('OID',where_clause_oid(v))
              wcd.append(range_check(v['testcd'], 'EQ', 'Soft', testcd_oid))
              wcds[key] = wcd
    return list(wcds.values())

DEFINE_XML = Path.cwd()  / "uploads" / "define.xml"

def main(uuid):
  print("start:", dt.now())
  try:
    study_info = get_study_info(uuid)
    domains = get_domains_and_variables(study_info['uuid'])

    define = {}
    root = ET.Element('ODM')
    odm_properties(root)
    study = set_study_info(study_name=study_info['study_name'])
    # Study -------->
    study.append(set_globalvariables(study_name=study_info['study_name'], study_description=study_info['rationale'], protocol_name=study_info['protocol_name']))

    # MetadataVersion -------->
    metadata = metadata_version(oid=study_info['uuid'], name=study_info['study_name'])

    # Standards
    metadata.append(ET.Comment("*********************************"))
    metadata.append(ET.Comment("Standard Definitions"))
    metadata.append(ET.Comment("*********************************"))
    metadata.append(standards())

    # Supporting documents
    # metadata.append(ET.Comment("*******************************"))
    # metadata.append(ET.Comment("Supporting Documents"))
    # metadata.append(ET.Comment("*******************************"))

    # def:ValueListDef
    metadata.append(ET.Comment("************************************************************************************************************************"))
    metadata.append(ET.Comment("Value List Definitions Section (Required for Supplemental Qualifiers, Optional for other Normalized (Vertical) Datasets) "))
    metadata.append(ET.Comment("(Note that any definitions not provided at a Value Level will be inherited from the parent item definition)"))
    metadata.append(ET.Comment("************************************************************************************************************************"))
    vlds = value_list_defs(domains)
    for vld in vlds:
      metadata.append(vld)

    # def:WhereClauseDef
    metadata.append(ET.Comment("*****************************************************************************"))
    metadata.append(ET.Comment("WhereClause Definitions Section (Used/Referenced in Value List Definitions)"))
    metadata.append(ET.Comment("*****************************************************************************"))
    wcds = where_clause_defs(domains)
    for wcd in wcds:  
      metadata.append(wcd)

    # ItemGroupDef
    metadata.append(ET.Comment("************************************************************************************"))
    metadata.append(ET.Comment("ItemGroupDef Definitions Section (Datasets and and first set of variable properties)"))
    metadata.append(ET.Comment("************************************************************************************"))
    igds = item_group_defs(domains)
    for igd in igds:
      metadata.append(igd)


    # ItemDef
    metadata.append(ET.Comment("*****************************************************************************"))
    metadata.append(ET.Comment("ItemDef Definitions Section (Variables and Value List, remaining properties)"))
    metadata.append(ET.Comment("*****************************************************************************"))
    idfs = item_defs_variable(domains)
    for idf in idfs:
      metadata.append(idf)
    idfs_test = vlm_item_defs(domains)
    for idf in idfs_test:
      metadata.append(idf)

    # CodeList
    metadata.append(ET.Comment("************************************"))
    metadata.append(ET.Comment("Code List Definitions Section"))
    metadata.append(ET.Comment("************************************"))
    codelists = codelist_defs(domains)
    for codelist in codelists:
      metadata.append(codelist)
    test_codelists = test_codes_defs(domains)
    for codelist in test_codelists:
      metadata.append(codelist)
    vlm_codelists = vlm_codelists_defs(domains)
    for codelist in vlm_codelists:
      metadata.append(codelist)

    # def:CommentDef
    comments = comment_defs()
    for comment in comments:
      metadata.append(comment)

    # # MethodDef
    # # def:leaf

    # MetadataVersion <--------
    # Study <--------
    study.append(metadata)
    root.append(study)

    # Generate ODM define
    tree = ET.ElementTree(root)
    ET.indent(tree, space="   ", level=0)
    tree.write(DEFINE_XML, encoding="utf-8")
    # Generate final define. Add stylesheet before rest of content
    with open(DEFINE_XML,'r') as f:
      lines = f.readlines()
    lines.insert(0,xml_header) 
    with open(DEFINE_XML,'w') as f:
      for line in lines:
         f.write(line)
    with open(DEFINE_XML,'r') as f:
      xml = f.read()
    print("end:", dt.now())
    return xml

  except Exception as e:
    print("Error",e)
    print(traceback.format_exc())
