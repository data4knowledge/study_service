from d4kms_service import Node, Neo4jConnection

class BaseNode(Node):
  uuid: str


  @classmethod
  def build_filter_clause(cls, filter, properties):
    filter_clause_parts = []
    for property in properties:
      filter_clause_parts.append("toLower(%s) CONTAINS toLower('%s')" % (property, filter))
    filter_clause = " OR ".join(filter_clause_parts)
    filter_clause = "WHERE %s " % (filter_clause)
    return filter_clause

  @classmethod
  def list(cls, page, size, filter):
    return cls.base_list(cls, f"MATCH (n:{cls.__name__})", "ORDER BY n.name ASC", page, size, filter)

  @classmethod
  def base_list(cls, base_query, order_clause, page, size, filter):
    if filter == "":
      count = cls.full_count(base_query)
    else:
      count = cls.filter_count(base_query, filter)
    results = {'items': [], 'page': page, 'size': size, 'filter': filter, 'count': count }
    results['items'] = cls.base_list_items(base_query, order_clause, page, size, filter)
    return results

  @classmethod
  def base_list_items(cls, base_query, order_clause, page=0, size=0, filter=""):
    ra = {}
    db = Neo4jConnection()
    with db.session() as session:
      results = []
      skip_offset_clause = ""
      if page != 0:
        offset = (page - 1) * size
        skip_offset_clause = "SKIP %s LIMIT %s" % (offset, size)
      if filter == "":
        query = """
          %s RETURN n %s %s
        """ % (base_query, order_clause, skip_offset_clause)
      else:
        parent_filter_clause = cls.build_filter_clause(filter, cls.parent_properties())
        query = """
          %s %s RETURN n %s %s
          }
        """ % (base_query, parent_filter_clause, order_clause, skip_offset_clause)
      #print(f"LIST: {query}")
      query_results = session.run(query)
      for query_result in query_results:
        rel = dict(query_result['n'])
        results.append(rel)
      return results

  @classmethod
  def full_count(cls, base_query):
    db = Neo4jConnection()
    with db.session() as session:
      query = """
        %s RETURN COUNT(n) as count 
      """ % (base_query)
      #print(f"FULL COUNT: {query}")
      query_results = session.run(query)
      for result in query_results:
        return result['count']
      return 0

  @classmethod
  def filter_count(cls, base_query, filter):
    db = Neo4jConnection()
    with db.session() as session:
      parent_filter_clause = cls.build_filter_clause(filter, cls.parent_properties())
      query = """
          %s %s RETURN n
      """ % (base_query, parent_filter_clause)
      #print(f"FILTER COUNT: {query}")
      query_results = session.run(query)
      return len(query_results.data())

class NodeId(BaseNode):
  id: str

  @classmethod
  def find_by_id(cls, label, id):
    db = Neo4jConnection()
    with db.session() as session:
      result = session.execute_read(cls._find_by_id, cls, label, id)
    db.close()
    return result

  @staticmethod
  def _find(tx, cls, label, id):
    node_clause = f"a:{label}"
    query = "MATCH (%s {id: $id }) RETURN a" % (node_clause)
    result = tx.run(query, id=id)
    for row in result:
      return cls.as_dict(row['a'])
    return None
  
class NodeName(NodeId):
  name: str

  @classmethod
  def parent_properties(cls):
    return ["n.name"]
  
class NodeNameLabel(NodeId):
  name: str
  label: str

  @classmethod
  def parent_properties(cls):
    return ["n.name", "n.label"]

class NodeDesc(NodeId):
  description: str
  
  @classmethod
  def parent_properties(cls):
    return ["n.description"]

class NodeNameLabelDesc(NodeId):
  name: str
  description: str
  label: str

  @classmethod
  def parent_properties(cls):
    return ["n.name", "n.description", "n.label"]
