import github
import os
import glob
import traceback
import logging
from uuid import uuid4
from d4kms_generic import ServiceEnvironment

class GithubService():
  
  class UploadFail(Exception):
    pass

  def __init__(self):
    self.token = ServiceEnvironment().get("GITHUB_TOKEN")
    self.repo_name = ServiceEnvironment().get("GITHUB_REPO")
    self.branch_name = ServiceEnvironment().get("GITHUB_BRANCH")
    self.g = github.Github(self.token)
    self.repo = self.g.get_repo(self.repo_name)
    self.files = []
    self.result = []
    self.file_count = 0
    self.file_index = 0
    self.file_remaining_count = 0
    self.elements = []

  def next(self, dir):
    try:
      file = self.files[self.file_index]
      with open(file, 'r') as f:
        data = f.read()
      name = os.path.basename(file)
      self.result.append(name)
      blob = self.repo.create_git_blob(data, "utf-8")
      element = github.InputGitTreeElement(path=f"{dir}/{file}", mode='100644', type='blob', sha=blob.sha)
      self.elements.append(element)
      self.file_count += 1
      self.file_index += 1
      self.file_remaining_count -= 1
      return self.file_remaining_count > 0    
    except Exception as e:
      logging.error(f"Exception {e} raised while uploading to Github, next\n{traceback.format_exc()}")
      raise self.UploadFail    

  def load(self):
    try:
      branch_sha = self.repo.get_branch(self.branch_name).commit.sha
      base_tree = self.repo.get_git_tree(sha=branch_sha)
      tree = self.repo.create_git_tree(self.elements, base_tree)
      parent = self.repo.get_git_commit(sha=branch_sha)
      commit = self.repo.create_git_commit("commit_message", tree, [parent])
      branch_refs = self.repo.get_git_ref(f"heads/{self.branch_name}")
      branch_refs.edit(sha=commit.sha)
    except Exception as e:
      logging.error(f"Exception {e} raised while uploading to Github, load\n{traceback.format_exc()}")
      raise self.UploadFail    

  def file_list(self, dir, file_type):
    try:
      self.files = glob.glob(os.path.join(dir, file_type))
      self.dir = dir
      self.file_remaining_count = len(self.files)
      return self.file_remaining_count
    except Exception as e:
      logging.error(f"Exception {e} raised while uploading to Github, file_list\n{traceback.format_exc()}")
      raise self.UploadFail    

  def upload_file_list(self):
    return self.result
  
  def progress(self):
    return self.file_count