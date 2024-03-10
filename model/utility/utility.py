import yaml
from d4kms_generic.logger import application_logger

def read_yaml_file(filepath):
  with open(filepath, "r") as f:
    data = yaml.load(f, Loader=yaml.FullLoader)
  return data

def read_text_file(filepath):
  with open(filepath, "r") as f:
    data = f.read()
  return data

def section_ordering(s):
  try:
    return [int(_) for _ in s[1]['section_number'].split(".")]
  except Exception as e:
    application_logger.exception("Exception during section ordering", e)