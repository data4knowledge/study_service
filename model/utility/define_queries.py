
def crm_link_query():
    query = """
      MATCH (d:Domain)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
      MATCH (bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
      MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)
      MATCH (v:Variable)-[:IS_A_REL]->(crm)
      MATCH (d:Domain)-[:VARIABLE_REL]->(v)
      RETURN distinct d.name as domain, bcp.name as bcp, crm.sdtm as crm, v.name as variable
      order by domain, variable
    """
    return query

def _add_missing_links_to_crm_query(uri, var):
    query = """
      MATCH (crm:CRMNode {uri:'%s'})
      MATCH (v:Variable {name:'%s'})
      with crm, v
      MERGE (v)-[r:IS_A_REL]->(crm)
      set r.fake_relationship = "yes"
      return "done" as done
    """ % (uri, var)
    return query

def study_info_query(uuid):
    query = """
    MATCH (sd:StudyDesign {uuid: '%s'})<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)
    OPTIONAL MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(:Organization {name:'Eli Lilly'})
    OPTIONAL MATCH (sv)-[:DOCUMENT_VERSION_REL]->(spdv:StudyProtocolDocumentVersion)<-[:VERSIONS_REL]->(spd:StudyProtocolDocument)
    return 
    sd.uuid as uuid,
    coalesce(si.studyIdentifier,"To be provided") as study_name,
    coalesce(sd.description, "To be provided") as description,
    sv.rationale as rationale,
    spd.name as protocol_name
    """ % (uuid)
    return query

def domains_query(uuid):
    query = """
      MATCH (sd:StudyDesign {uuid: '%s'})-[]->(d:Domain) RETURN d
      ORDER BY d.name
    """ % (uuid)
    return query


def define_vlm_query(domain_uuid):
    query = """
      MATCH (sd:StudyDesign)-[:DOMAIN_REL]->(domain:Domain {uuid:'%s'})
      MATCH (sd)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)
      MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(:Organization {name:'Eli Lilly'})
      WITH si, domain
      MATCH (domain)-[:USING_BC_REL]-(bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
      MATCH (bc)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(cd:Code)
      WHERE EXISTS {
          (bcp)<-[:PROPERTIES_REL]->(dc:DataContract)
      }
      WITH bc, cd, bcp, domain
      MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(domain)
      MATCH (bcp)-[:RESPONSE_CODES_REL]->(rc:ResponseCode)-[:CODE_REL]->(c:Code)
      where bcp.name = var.name or bcp.label = var.label
      WITH domain, bc, cd, bcp, crm, var, c
      ORDER By bc.name, cd.decode, bcp.name, c.decode
      WITH distinct domain.name as domain
      , bc.uuid as bc_uuid
      , bc.name as bc_name
      , cd.decode as testcd
      , bcp.name as bcp_name
      , crm.datatype as datatype
      , var.uuid as var_uuid
      , var.label as label
      , var.data_type as var_datatype
      , var.name as name
      , var.core as core
      , var.ordinal as ordinal
      , c.code as code
      , c.notation as notation
      , c.pref_label as pref_label
      , c.decode as decode
      return 
      domain,
      bc_uuid,
      bc_name,
      testcd,
      bcp_name,
      datatype,
      var_uuid,
      label,
      var_datatype,
      name,
      core,
      ordinal,
      collect({code:code, decode:decode, code:code, pref_label:pref_label}) as decodes
    """ % (domain_uuid)
    # limit 100
    # print("vlm query", query)
    return query


def domain_variables_query(uuid):
    query = """
      MATCH (d:Domain {uuid: '%s'})-[]->(v:Variable) RETURN v
      ORDER BY v.ordinal
    """ % (uuid)
    return query


def variables_crm_link_query(uuid):
    query = """
      MATCH (d:Domain {uuid: '%s'})-[]->(v:Variable)-[:IS_A_REL]->(:CRMNode) RETURN v
      ORDER BY v.name
    """ % (uuid)
    # print("variables query", query)
    return query


def define_codelist_query(domain_uuid):
    # query = """
    #   MATCH (sd:StudyDesign)-[:DOMAIN_REL]->(domain:Domain {uuid:'%s'})
    #   MATCH (sd)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)
    #   MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(:Organization {name:'Eli Lilly'})
    #   WITH si, domain
    #   MATCH (domain)-[:USING_BC_REL]-(bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
    #   MATCH (bc)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(bc_cd:Code)
    #   WITH bc, bc_cd, bcp, domain
    #   MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(domain)
    #   MATCH (bcp)-[:RESPONSE_CODES_REL]->(rc:ResponseCode)-[:CODE_REL]->(c:Code)
    #   // WHERE  var.label = bcp.label
    #   // or bcp.name = crm.sdtm
    #   WITH bc, bc_cd, bcp, crm, var, c
    #   ORDER By bc.name, bc_cd.decode, bcp.name, c.decode
    #   WITH bc, bc_cd, bcp, crm, var, collect({code:c.code,decode:c.decode}) as decodes
    #   return distinct 
    #   // bc.uuid as bc_uuid,
    #   bc.name as bc,
    #   bc_cd.decode as testcd,
    #   var.uuid as uuid,
    #   var.label as label,
    #   var.name as name,
    #   crm.datatype as datatype,
    #   var.ordinal as ordinal,
    #   decodes as decodes
    #   order by name
    # """ % (domain_uuid)
    query = """
        MATCH (sd:StudyDesign)-[:DOMAIN_REL]->(domain:Domain {uuid:'%s'})
        MATCH (sd)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)
        MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(:Organization {name:'Eli Lilly'})
        WITH si, domain
        MATCH (domain)-[:USING_BC_REL]-(bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
        MATCH (bc)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(bc_cd:Code)
        MATCH (bcp)-[:IS_A_REL]->(crm:CRMNode)<-[:IS_A_REL]-(var:Variable)<-[:VARIABLE_REL]-(domain)
        MATCH (bcp)-[:RESPONSE_CODES_REL]->(rc:ResponseCode)-[:CODE_REL]->(c:Code)
        WHERE bcp.name = var.name or bcp.label = var.label
        with distinct var.uuid as uuid, var.label as label, var.name as name, var.ordinal as ordinal, c.code as code, c.decode as decode, c.notation as notation, c.pref_label as pref_label
        WITH uuid, label, name, ordinal, collect({code:code, decode: decode, notation: notation, pref_label: pref_label}) as decodes
        return uuid, label, name, ordinal, decodes
    """ % (domain_uuid)
    return query

def define_test_codes_query(domain_uuid):
    query = """
      MATCH (sd:StudyDesign)-[:DOMAIN_REL]->(domain:Domain {uuid:'%s'})
      MATCH (sd)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)
      MATCH (sv)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)-[:STUDY_IDENTIFIER_SCOPE_REL]->(:Organization {name:'Eli Lilly'})
      WITH si, domain
      MATCH (domain)-[:USING_BC_REL]-(bc:BiomedicalConcept)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
      MATCH (bc)-[:CODE_REL]-(:AliasCode)-[:STANDARD_CODE_REL]->(bc_cd:Code)
      WITH distinct domain.name as domain, bc_cd.decode as testcd, bc.name as test, bc_cd.code as code
      ORDER By domain, testcd, test
      WITH domain, collect({code:code, testcd:testcd, test:test}) as test_codes
      return domain, test_codes
    """ % (domain_uuid)
    return query

def find_ct_query(identifiers):
    query = """
      use `ct-service-dev`
      MATCH (ss:SkosConceptScheme)-[:TOP_LEVEL_CONCEPT]-(cl:SkosConcept)-[:NARROWER]->(c:SkosConcept)
      WHERE NOT EXISTS {
      (c)<-[:PREVIOUS]-()
      }
      AND not EXISTS {
      (ss)<-[:PREVIOUS]-()    
      }
      and c.identifier in %s
      and ss.uri contains 'sdtm'
      return distinct c.identifier as code, c.pref_label as pref_label, c.notation as notation
    """ % (identifiers)
    return query
