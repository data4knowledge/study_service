import os
import glob
from d4kms_generic import application_logger
from d4kms_generic import ServiceEnvironment

class UploadService():
  
  class UploadFail(Exception):
    pass

  def __init__(self):
    self.service_environment = ServiceEnvironment()
    self.files = []
    self.short_filenames = []

  def file_list(self, dir, file_type):
    try:
      self.files = glob.glob(os.path.join(dir, file_type))
      self.dir = dir
      self.file_remaining_count = len(self.files)
      self.result = [os.path.basename(file) for file in self.files]
      return self.file_remaining_count
    except Exception as e:
      application_logger.exception(f"Exception raised while building file list for upload", e)
      raise self.UploadFail    

  def upload_file_list(self):
    return self.short_filenames
  
