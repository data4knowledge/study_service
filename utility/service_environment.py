from dotenv import load_dotenv
import os
import logging

class ServiceEnvironment():
  
  def __init__(self):
    self.load()

  def environment(self):
    if 'PYTHON_ENVIRONMENT' in os.environ:
      return os.environ['PYTHON_ENVIRONMENT']
    else:
      return "development"

  def production(self):
    return self.environment() == "production"

  def get(self, name):
    if name in os.environ:
      return os.environ[name]
    else:
      logging.info("Missing environment variable detected: %s" % (name))
      return ""

  def load(self):
    load_dotenv(".%s_env" % self.environment())
