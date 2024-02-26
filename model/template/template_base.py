
import yaml

class TemplateBase():
   
  def read_yaml_file(self, filepath):
    with open(filepath, "r") as f:
      data = yaml.load(f, Loader=yaml.FullLoader)
    return data
  