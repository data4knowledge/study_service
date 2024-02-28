import yaml

def read_yaml_file(filepath):
  with open(filepath, "r") as f:
    data = yaml.load(f, Loader=yaml.FullLoader)
  return data

def read_text_file(filepath):
  with open(filepath, "r") as f:
    data = f.read()
  return data
