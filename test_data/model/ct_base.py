from model.utility.utility import read_yaml_file

class CTBase():

  def __init__(self, filename, default):
    self.items = read_yaml_file(filename)['items']
    self.default_pt = default

  def get(self, preferred_term):
    item = next((x for x in self.items if x['preferredTerm'] == preferred_term), None)
    if not item:
      item = next((x for x in self.items if x['preferredTerm'] == self.default_pt), None)
    return item
  
  def default(self):
    return next((x for x in self.items if x['preferredTerm'] == self.default_pt), None)
