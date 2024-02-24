
import yaml
from model.base_node import N
class TemplateBase(Node):
   
  def read_yaml_file(self, filepath):
    with open(filepath, "r") as f:
      data = yaml.load(f, Loader=yaml.FullLoader)
    return data
  