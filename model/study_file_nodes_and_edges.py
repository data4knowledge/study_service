import csv
import os
import glob
import traceback
from stringcase import snakecase 

class StudyFileNodesAndEdges():

  def __init__(self, dir, data):
    self.nodes = {}
    self.edges = {}
    self.id = 1
    self.uuid_to_id = {}
    self.dir = dir
    for k, v in data['nodes'].items():
      self.add_nodes(k, v)
    for k,v in data['edges'].items():
      self.add_edges(k, v)

  def add_nodes(self, klass, items):
    print(f"NE (Add Nodes): {items}")
    if not klass in self.nodes:
      self.nodes[klass] = []
    self.nodes[klass] = self.nodes[klass] + items

  def add_edges(self, label, items):
    #print(f"NE (Add Edges): {items}")
    if not label in self.edges:
      self.edges[label] = []
    new_items = [{"from": x['start'], "to": x['end']} for x in items]
    self.edges[label] = self.edges[label] + new_items

  def dump(self):
    try:
      for k, v in self.nodes.items():
        print("NODE '%s': %s" % (k, len(v)))
        self.dump_nodes(k, v)
      for k, v in self.edges.items():
        print("REL '%s': %s" % (k, len(v)))
        self.dump_edges(k, v)
    except Exception as e:
      print(f"NE: Dump exception {e}")
      print(f"{traceback.format_exc()}")

  def clean(self):
    files = glob.glob("%s/*.csv" % (self.dir))
    for f in files:
      os.remove(f)
  
  def dump_nodes(self, type, data, block_size=100000):
    block_count = 0
    file_count = 1
    fields = self.unique_keys(data)
    #fieldnames = ["id:ID"] + fields
    for row in data:
      self.fill_missing(type, fields, row)
      if block_count == 0:
        csv_filename = "%s/node-%s-%s.csv" % (self.dir, snakecase(type), file_count)
        csv_file = open(csv_filename, mode='w', newline='')
        writer = csv.DictWriter(csv_file, fieldnames=fields, quoting=csv.QUOTE_ALL, lineterminator="\n")
        writer.writeheader()
        file_count += 1
      #row["id:ID"] = self.id
      #self.uuid_to_id[row["uuid"]] = self.id
      #self.id += 1
      writer.writerow(row)
      block_count += 1
      if block_count >= block_size:
        block_count = 0

  def dump_edges(self, type, items, block_size=100000):
    block_count = 0
    file_count = 1
    fieldnames = [ ":START_ID", ":END_ID" ]
    for row in items:
      if block_count == 0:
        csv_filename = "%s/rel-%s-%s.csv" % (self.dir, type.lower(), file_count)
        csv_file = open(csv_filename, mode='w', newline='')
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, lineterminator="\n")
        writer.writeheader()
        file_count += 1
      #new_row = { ":START_ID": self.uuid_to_id[row["from"]], ":END_ID": self.uuid_to_id[row["to"]] }
      new_row = { ":START_ID": row["from"], ":END_ID": row["to"] }
      writer.writerow(new_row)
      block_count += 1
      if block_count >= block_size:
        block_count = 0

  def unique_keys(self, arr):
    keys = list(set(val for dic in arr for val in dic.keys()))
    for i,key in enumerate(keys):
      if key == "uuid":
        keys[i] = "uuid:ID"
    return keys
  
  def fill_missing(self, type, keys, row):
    row['uuid:ID'] = row.pop('uuid')
    for k in keys:
      if k not in row:
        row[k] = ''
        print(f"NE (fill missing): {type} missing {k}")
