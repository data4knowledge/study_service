from github import Github
from utility.service_environment import ServiceEnvironment

import os
import glob
import time
import traceback

class GithubService():
  
  class UploadFail(Exception):
    pass

  def __init__(self):
    self.token = ServiceEnvironment().get("GITHUB_TOKEN")
    self.repo_name = ServiceEnvironment().get("GITHUB_REPO")
    self.branch_name = ServiceEnvironment().get("GITHUB_BRANCH")
    self.g = Github(self.token)
    self.repo = self.g.get_repo(self.repo_name)
 
  def upload_dir(self, uuid, dir, file_type):
    result = []
    try:
      print(f"GIT: Dir {dir}")
      files = glob.glob(os.path.join(dir, file_type))
      print(f"GIT: Files {files}")
      for file in files:
        print(f"GIT: File {file}")
        with open(file, 'r') as f:
          data = f.read()
        name = os.path.basename(file)
        filename = os.path.join(dir, name)
        self.repo.create_file(filename, 'Study service excel import', data, branch=self.branch_name)
        result.append(name)
    except Exception as e:
      print(f"GIT: Upload exception {e}")
      print(f"{traceback.format_exc()}")
    else:
      self._all_visible(dir, len(result))
      return result
        
  def _all_visible(self, dir, target_count):
    check = True
    loop_count = 0
    while check and (loop_count <= 12):
      file_count = self._file_count(dir)
      if file_count >= target_count:
        check = False
      else:
        time.sleep(5)
        loop_count += 1
    if check:
      raise self.UploadFail
  
  def _file_count(self, dir):
    count = 0
    contents = self.repo.get_contents(dir, ref=self.branch_name)
    while contents:
      file_content = contents.pop(0)
      if file_content.type == "dir":
        contents.extend(self.repo.get_contents(file_content.path, ref=self.branch_name))
      else:
        count += 1
    return count
  

