import re
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
  keywords = {
    'appendix': '1000.'
  }
  try:
    text = s[1]['section_number']
    text = text[:-1] if text.endswith('.') else text
    for keyword, section in keywords.items():
      if keyword in text.lower():
        text = text.lower().replace(keyword, section)
    text = re.sub('[^\d\.]', '', text)
    #print(f"ORDER: {s[1]['section_number']}={text}")
    return [int(_) for _ in text.split(".")]
  except Exception as e:
    application_logger.exception("Exception during section ordering", e)