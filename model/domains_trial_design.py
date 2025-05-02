import re
import pandas as pd
from d4kms_service import Neo4jConnection
from d4kms_generic import application_logger
from bs4 import BeautifulSoup

class TrialDesignDomain():

  @classmethod
  def create(self, uuid, domain_name, page, size, filter):
    results = []
    if domain_name == 'TA':
      results = self.trial_arms(uuid)
    elif domain_name == 'TS':
      results = self.trial_summary(uuid)
    elif domain_name == 'TE':
      results = self.trial_elements(uuid)
    elif domain_name == 'TI':
      results = self.trial_inclusion_exclusion(uuid)
    elif domain_name == 'TV':
      results = self.trial_visits(uuid)
    else:
      print("Got unknown trial design domain", domain_name)
      results = {}
    return results

  @classmethod
  def trial_arms(cls, sd_uuid):
    try:
      response = cls._trial_arms(sd_uuid)
      if len(response) == 0:
        return {'table': []}
      df = pd.DataFrame(columns=response[0].keys())
      for item in response:
        df.loc[str(len(df.index))] = item
      # print(df.head())
      table = df.to_dict('index')
      # return {'table': table, 'groups': groups, 'mermaid_graph': mermaid_graph}
      return {'table': table}
    except Exception as e:
      print("Error: creating TA dataframe", e)
      return {f'error': f"Could not create TA dataframe ({e})"}

  @classmethod
  def trial_elements(cls, sd_uuid):
    try:
      response = cls._trial_elements(sd_uuid)
      if len(response) == 0:
        return {'table': []}
      df = pd.DataFrame(columns=response[0].keys())
      for item in response:
        df.loc[str(len(df.index))] = item
      table = df.to_dict('index')
      return {'table': table}
    except Exception as e:
      print("Error: creating TE dataframe", e)
      return {f'error': 'Could not create TE dataframe ({e})'}

  @classmethod
  def trial_visits(cls, sd_uuid):
    try:
      study_id = cls._study_id(sd_uuid)
      response = cls._trial_visits(sd_uuid, study_id)
      if len(response) == 0:
        return {'table': []}
      df = pd.DataFrame(columns=response[0].keys())
      for item in response:
        df.loc[str(len(df.index))] = item
      table = df.to_dict('index')
      return {'table': table}
    except Exception as e:
      print("Error: creating TV dataframe", e)
      return {f'error': 'Could not create TV dataframe ({e})'}

  @classmethod
  def trial_inclusion_exclusion(cls, sd_uuid):
    try:
      study_id = cls._study_id(sd_uuid)
      response = cls._trial_inclusion_exclusion(sd_uuid, study_id)
      for item in response:
        usdm_tags = re.findall(r'(<usdm:tag name=.+?/>)', item['IETEST'])
        for usdm_tag in usdm_tags:
          reference_text = cls._get_tag_text(sd_uuid, usdm_tag)
          item['IETEST'] = item['IETEST'].replace(usdm_tag, reference_text)
      if len(response) == 0:
        return {'table': []}
      df = pd.DataFrame(columns=response[0].keys())
      for item in response:
        df.loc[str(len(df.index))] = item
      table = df.to_dict('index')
      return {'table': table}
    except Exception as e:
      print("Error: creating TI dataframe", e)
      return {f'error': 'Could not create TI dataframe ({e})'}

  @classmethod
  def trial_summary(cls, sd_uuid):
    try:
      study_id = cls._study_id(sd_uuid)
      response = cls._trial_summary(sd_uuid)
      transposed = []
      for k,v in response[0].items():
        seq = 1
        if ';' in v:
          for v2 in  v.split(';'):
            item = {}
            item['STUDYID'] = study_id
            item['DOMAIN'] = 'TS'
            item['TSSEQ'] = seq
            item['TSPARM'] = k
            item['TSVAL'] = v2
            transposed.append(item)
            seq += 1
        else:
          item = {}
          item['STUDYID'] = study_id
          item['DOMAIN'] = 'TS'
          item['TSSEQ'] = seq
          item['TSPARM'] = k
          item['TSVAL'] = v
          transposed.append(item)
        # print(item)

      df = pd.DataFrame(columns=transposed[0].keys())
      for item in transposed:
        df.loc[str(len(df.index))] = item
      table = df.to_dict('index')
      return {'table': table }
    except Exception as e:
      return {'error': 'Could not create TS dataframe'}

  @staticmethod
  def _get_tag_text(sd_uuid, usdm_tag):
    db = Neo4jConnection()
    results = re.search(r'"(.+?)"', usdm_tag)
    reference_txt = "(linked text not found)"
    if results:
      tag_name = results.group(1)
      with db.session() as session:

        # Get tag node which contains the xml reference
        query = """
          match (sd:StudyDesign {uuid: '%s'})-[:DICTIONARIES_REL]->(:SyntaxTemplateDictionary)-[:PARAMETER_MAPS_REL]->(n {tag: '%s'}) return n['reference'] as reference
        """ % (sd_uuid, tag_name)
        # print("tag query", query)
        results = session.run(query)
        for result in results.data():
          reference_xml = result['reference']

        # Get reference node from the xml reference
        soup = BeautifulSoup(reference_xml, "html.parser")
        reference = soup.find("usdm:ref")
        query = """match (:StudyDesign {uuid: "%s"})-[*1..2]->(n:%s {id:'%s'}) return n.%s as txt""" % (sd_uuid, reference.attrs['klass'], reference.attrs['id'], reference.attrs['attribute'])
        # print("reference query", query)
        results = session.run(query)
        for result in results.data():
          reference_txt = result['txt']
      db.close()
    # print("reference_txt", reference_txt)
    return reference_txt

  @staticmethod
  def _study_id(sd_uuid):
    db = Neo4jConnection()
    study_id = 'not found'
    with db.session() as session:
      query = """
        match (sd:StudyDesign {uuid:'%s'})-[:ARMS_REL]-(a:StudyArm)
        match (sd)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)
        match (si)-[:STUDY_IDENTIFIER_SCOPE_REL]-(o:Organization {name:'Eli Lilly'})
        return  si.studyIdentifier as STUDYID
      """ % (sd_uuid)
      # print("study id query", query)
      results = session.run(query)
      # print("results",results)
      for result in results.data():
        study_id = result['STUDYID']
    db.close()
    return study_id

  @staticmethod
  def _trial_arms(sd_uuid):
    db = Neo4jConnection()
    data = []
    with db.session() as session:
      query = """
        match (sd:StudyDesign {uuid:'%s'})-[:ARMS_REL]-(a:StudyArm)
        match (sd)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)
        match (a)-[:TYPE_REL]-(c:Code)
        match (a)<-[:ARM_REL]-(sc:StudyCell)
        match (sc)-[:EPOCH_REL]->(epoch:StudyEpoch)
        match (sc)-[:ELEMENT_REL]->(el:StudyElement)
        match (el)-[:TRANSITION_START_RULE_REL]->(trs:TransitionRule)
        match (si)-[:STUDY_IDENTIFIER_SCOPE_REL]-(o:Organization {name:'Eli Lilly'})
        optional match (el)-[:TRANSITION_END_RULE_REL]->(tr_branch:TransitionRule)
        with
          si.studyIdentifier as STUDYID
          ,'TA' as DOMAIN
          , toInteger(split(epoch.id,'_')[1]) as epoch_ord
          , toInteger(split(el.id,'_')[1]) as element_ord
          , a.name as ARM
          // , c.decode as type
          , el.name as ETCD
          , el.label as ELEMENT
          , epoch.name as EPOCH
          , coalesce(tr_branch.text,"") as TATRANS 
          , trs.text as TABRANCH
          , epoch.label as epoch_label
            order by ARM, epoch_ord, element_ord
        return
          STUDYID
          ,DOMAIN
          ,ARM
          ,ETCD
          ,ELEMENT
          ,TATRANS
          ,TABRANCH
          ,EPOCH
      """ % (sd_uuid)
      # print("trial arms query", query)
      results = session.run(query)
      first_data = [x.data() for x in results]
      last_arm = None
      row = -99
      for x in first_data:
        if x['ARM'] == last_arm:
          row += 1
          x['TAETORD'] = row
        else:
          last_arm = x['ARM']
          row = 1
          x['TAETORD'] = row
      data = []
      for x in first_data:
        item = {}
        item['STUDYID'] = x['STUDYID']
        item['DOMAIN'] = x['DOMAIN']
        item['ARM'] = x['ARM']
        item['TAETORD'] = x['TAETORD']
        item['ETCD'] = x['ETCD']
        item['ELEMENT'] = x['ELEMENT']
        item['TABRANCH'] = x['TABRANCH']
        item['TATRANS'] = x['TATRANS']
        item['EPOCH'] = x['EPOCH']
        data.append(item)

    db.close()
    return data

  @staticmethod
  def _trial_elements(sd_uuid):
    db = Neo4jConnection()
    data = []
    with db.session() as session:
      query = """
        match (sd:StudyDesign {uuid:'%s'})-[:ARMS_REL]-(a:StudyArm)
        match (sd)<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)
        match (a)-[:TYPE_REL]-(c:Code)
        match (a)<-[:ARM_REL]-(sc:StudyCell)
        match (sc)-[:ELEMENT_REL]->(el:StudyElement)
        optional match (el)-[:TRANSITION_START_RULE_REL]->(trs:TransitionRule)
        optional match (el)-[:TRANSITION_END_RULE_REL]->(tre:TransitionRule)
        match (si)-[:STUDY_IDENTIFIER_SCOPE_REL]-(o:Organization {name:'Eli Lilly'})
        with distinct
          si.studyIdentifier as STUDYID
          ,'TE' as DOMAIN
          , el.name as ETCD
          , el.label as ELEMENT
          ,trs.text as TESTRL
          ,coalesce(tre.text,"") as TEENRL
          ,'' as TEDUR
        return
          STUDYID
          ,DOMAIN
          ,ETCD
          ,ELEMENT
          ,TESTRL
          ,TEENRL
          ,TEDUR
        order by ETCD
      """ % (sd_uuid)
      # print("trial arms query", query)
      results = session.run(query)
      data = [x.data() for x in results]
    db.close()
    return data

  @staticmethod
  def _trial_inclusion_exclusion(sd_uuid, study_id):
    db = Neo4jConnection()
    data = []
    with db.session() as session:
      query = """
        match (sd:StudyDesign {uuid:'%s'})<-[:STUDY_DESIGNS_REL]-(sv:StudyVersion)-[:STUDY_IDENTIFIERS_REL]->(si:StudyIdentifier)
        with si, sv, sd
        match (sd)-[:POPULATION_REL]-(p:StudyDesignPopulation)-[:CRITERIA_REL]->(ie:EligibilityCriterion)-[:CATEGORY_REL]->(c:Code)
        RETURN distinct
          '%s'                  as STUDYID
          ,'TI'                 as DOMAIN
          ,ie.name              as IETESTCD
          ,ie.text              as IETEST
          ,c.decode             as IECAT
          ,''                   as TIRL
          ,sv.versionIdentifier as TIVERSION
        order by IETESTCD
      """ % (sd_uuid, study_id)
      # print("trial arms query", query)
      results = session.run(query)
      data = [x.data() for x in results]
    db.close()
    return data

  @staticmethod
  def _trial_visits(sd_uuid, study_id):
    db = Neo4jConnection()
    data = []
    with db.session() as session:
      query = """
        MATCH (sd:StudyDesign {uuid: '%s'})-[:SCHEDULE_TIMELINES_REL]->(stl:ScheduleTimeline {mainTimeline: 'True'})-[:ENTRY_REL]-(sai1:ScheduledActivityInstance)
        WITH sai1
        MATCH path=(sai1)-[:DEFAULT_CONDITION_REL *0..]->(sai)
        WITH sai, LENGTH(path) as a_ord
        MATCH (sai)-[:ENCOUNTER_REL]->(e:Encounter)
        MATCH (sai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]->(t:Timing)
        OPTIONAL MATCH (t)-[:RELATIVE_TO_SCHEDULED_INSTANCE_REL]-(sai_to:ScheduledActivityInstance)
        OPTIONAL MATCH (t)-[:RELATIVE_TO_FROM_REL]-(from_rel:Code)
        OPTIONAL MATCH (t)-[:TYPE_REL]-(type_rel:Code)
        return 
        '%s' as STUDYID
        , 'TV' as DOMAIN
        // , toInteger(split(e.id,'_')[1]) as VISITNUM
        , toInteger(split(t.name,'TIM')[1]) as VISITNUM
        // , e.name as e_name
        , e.label+" - "+t.label as VISIT
        // , sai.name as sai_name
        // , sai.label as sai_label
        // , t.label as t_label
        // , t.value as t_value
        // , t.valueLabel as t_valueLabel
        // , from_rel.decode as from_rel
        // , type_rel.decode as type_rel
        // , sai_to.label as rel_to_sai
        // , t.label as VISITDY
        , '' as ARMCD
        , t.valueLabel+" "+type_rel.decode+" "+sai_to.label as TVSTRL
        , '' as TVENRL
        ORDER BY VISITNUM
      """ % (sd_uuid, study_id)
      # print("trial visits query", query)
      results = session.run(query)
      data = [x.data() for x in results]
    db.close()
    return data

  @staticmethod
  def _trial_summary(sd_uuid):
    db = Neo4jConnection()
    data = []
    with db.session() as session:
      query = """
        match(s:Study)-[r1:VERSIONS_REL]->(sv:StudyVersion)-[r4:STUDY_DESIGNS_REL]->(design:StudyDesign {uuid: '%s'})
        WITH s, sv, r1, design
        OPTIONAL match(sv)-[r2:DATE_VALUES_REL]->(gov_date:GovernanceDate) 
        OPTIONAL MATCh (sv)-[r3:TITLES_REL]->(title:StudyTitle)
        OPTIONAL MATCh (sv)-[r5:AMENDMENTS_REL]->(amend:StudyAmendment)
        OPTIONAL MATCh (sv)-[r6:STUDY_TYPE_REL]->(type:Code)
        OPTIONAL MATCh (sv)-[r7:STUDY_PHASE_REL]->(code:AliasCode)-[r7_1:STANDARD_CODE_REL]->(phase:Code)
        OPTIONAL MATCh (sv)-[r8:BUSINESS_THERAPEUTIC_AREAS_REL]->(ta:Code)
        OPTIONAL MATCh (sv)-[r9:DOCUMENT_VERSION_REL]->(doc_ver:StudyProtocolDocumentVersion)
        OPTIONAL MATCh (sv)-[r10:STUDY_IDENTIFIERS_REL]->(studyid:StudyIdentifier)
        return s.name as StudyName,
        apoc.text.join(collect(distinct studyid.studyIdentifier ),';')as StudyIDs,
        apoc.text.join(collect(distinct title.text),';') as Titles,
        type.decode as StudyType,
        phase.decode as Phase,
        ta.decode as BusinessTA,
        sv.versionIdentifier as Version,
        sv.rationale as StudyVersionRationale,
        gov_date.label as GovernanceType,
        gov_date.dateValue as GovernanceDate,
        doc_ver.protocolVersion as ProtocolVersion,
        amend.summary as AmendmentSummery,
        amend.number as AmendmendNumber,
        design.name as StudyDesign
      """ % (sd_uuid)
      # print("trial summary query", query)
      results = session.run(query)
      data = [x.data() for x in results]
    db.close()
    return data

  def print_dataframe(self, title, df):
    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    print(title)
    print("")
    print(df)
    print("")
    print("")
    print("")
