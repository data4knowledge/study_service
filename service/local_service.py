import os
import github
from service.upload_service import UploadService
from d4kms_generic import application_logger

class LocalService(UploadService):
  
  def __init__(self):
    super().__init__()
    self.project_root = self.service_environment.get("LOCAL_BASE")
    self.file_count = 0
    self.file_index = 0
    self.file_remaining_count = 0
    self.elements = []

  def next(self):
    try:
      file = self.files[self.file_index]
      self.file_count += 1
      self.file_index += 1
      self.file_remaining_count -= 1
      return self.file_remaining_count > 0    
    except Exception as e:
      application_logger.exception(f"Exception raised while processing next file for Github", e)
      raise self.UploadFail    

  def load(self):
    try:
      pass
    except Exception as e:
      application_logger.exception(f"Exception raised while uploading to Github", e)
      raise self.UploadFail    

  def upload_file_list(self, dir):
    return [{'filename': x, 'file_path': os.path.join(self.project_root, dir, 'load', x)} for x in self.short_filenames]
  
  def progress(self):
    return self.file_count