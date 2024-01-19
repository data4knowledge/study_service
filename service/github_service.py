from github import Github
from d4kms_generic import ServiceEnvironment

import os
import glob
import time
import traceback
import logging

class GithubService():
  
  class UploadFail(Exception):
    pass

  def __init__(self):
    self.token = ServiceEnvironment().get("GITHUB_TOKEN")
    self.repo_name = ServiceEnvironment().get("GITHUB_REPO")
    self.branch_name = ServiceEnvironment().get("GITHUB_BRANCH")
    self.g = Github(self.token)
    self.repo = self.g.get_repo(self.repo_name)
    self.files = []
    self.file_count = 0
    self.file_index = 0
    self.file_remaining_count = 0
    self.result = []

  def upload_dir(self, uuid, dir, file_type):
    try:
      self.files = glob.glob(os.path.join(dir, file_type))
      self.file_index = 0
      self.file_count = 0
      self.file_remaining_count = len(self.files)
      self.file_target_count = self.file_remaining_count
      self.dir = dir
      return len(self.files)
    except Exception as e:
      logging.error(f"Exception raised while uploading to Github, uploading")
      logging.error(f"Exception {e}\n{traceback.format_exc()}")
      raise self.UploadFail    

  def next(self):
    try:
      file = self.files[self.file_index]
      with open(file, 'r') as f:
        data = f.read()
      name = os.path.basename(file)
      #filename = os.path.join(dir, name)
      #self.repo.create_file(filename, 'Study service excel import', data, branch=self.branch_name)
      self.repo.create_file(file, 'Study service excel import', data, branch=self.branch_name)
      self.result.append(name)
      logging.info(f"Uploaded {file} to Github")    
      self.file_index += 1
      self.file_count += 1
      self.file_remaining_count -= 1
      return self.file_remaining_count > 0
    except Exception as e:
      logging.error(f"Exception raised while uploading to Github, uploading")
      logging.error(f"Exception {e}\n{traceback.format_exc()}")
      raise self.UploadFail    

  def upload_file_list(self):
    return self.result
  
  def progress(self):
    return self.file_count

  def check_all_visible(self):
    check = True
    loop_count = 0
    while check and (loop_count <= 12):
      file_count = self._file_count()
      if file_count >= self.file_target_count:
        check = False
      else:
        time.sleep(5)
        loop_count += 1
    if check:
      logging.error(f"Exception raised while uploading to Github, checking files")
      logging.error(f"Exception {e}\n{traceback.format_exc()}")
      raise self.UploadFail
  
  def _file_count(self):
    count = 0
    contents = self.repo.get_contents(self.dir, ref=self.branch_name)
    while contents:
      file_content = contents.pop(0)
      if file_content.type == "dir":
        contents.extend(self.repo.get_contents(file_content.path, ref=self.branch_name))
      else:
        count += 1
    return count
