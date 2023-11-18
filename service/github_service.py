from github import Github
from utility.service_environment import ServiceEnvironment

import os
import glob
import traceback

class GithubService():
    
  def __init__(self):
    self.token = ServiceEnvironment().get("GITHUB_TOKEN")
    self.repo_name = ServiceEnvironment().get("GITHUB_REPO")
    self.branch_name = ServiceEnvironment().get("GITHUB_BRANCH")
    self.g = Github(self.token)
    self.repo = self.g.get_repo(self.repo_name)
 
  def upload_dir(self, uuid, dir, file_type):
    try:
      result = []
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
      return result
    except Exception as e:
      print(f"GIT: Upload exception {e}")
      print(f"{traceback.format_exc()}")

  def file_count(self, uuid):
    count = 0
    contents = self.repo.get_contents(f'uploads/{uuid}', ref=self.branch_name)
    while contents:
      file_content = contents.pop(0)
      if file_content.type == "dir":
        contents.extend(self.repo.get_contents(file_content.path, ref=self.branch_name))
      else:
        count += 1
    return count