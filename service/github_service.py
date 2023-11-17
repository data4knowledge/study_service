from github import Github
from utility.service_environment import ServiceEnvironment

import os
import glob
import traceback

class GithubService():
    
  def __init__(self):
    self.token = ServiceEnvironment().get("GITHUB_TOKEN")
    self.repo_name = ServiceEnvironment().get("GITHUB_REPO")
    self.g = Github(self.token)
    self.repo = self.g.get_repo(self.repo_name)

  def upload_dir(self, uuid, dir, file_type):
    try:
      print(f"GIT: Dir {dir}")
      files = glob.glob(os.path.join(dir, file_type))
      print(f"GIT: Files {files}")
      for file in files:
        print(f"GIT: File {file}")
        with open(file, 'r') as f:
          data = f.read()
        name = os.path.basename(file)
        self.repo.create_file(f'uploads/{uuid}/{name}', 'Study service excel import', data, branch='aura')
    except Exception as e:
      print(f"GIT: Upload exception {e}")
      print(f"{traceback.format_exc()}")
