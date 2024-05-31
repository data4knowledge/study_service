import os
import github
from service.upload_service import UploadService
from d4kms_generic import application_logger

class GithubService(UploadService):
  
  def __init__(self):
    super().__init__()
    self.token = self.service_environment.get("GITHUB_TOKEN")
    self.repo_name = self.service_environment.get("GITHUB_REPO")
    self.branch_name = self.service_environment.get("GITHUB_BRANCH")
    self.project_root = self.service_environment.get("GITHUB_BASE")
    self.g = github.Github(self.token)
    self.repo = self.g.get_repo(self.repo_name)
    self.file_count = 0
    self.file_index = 0
    self.file_remaining_count = 0
    self.elements = []

  def next(self):
    try:
      file = self.files[self.file_index]
      with open(file, 'r') as f:
        data = f.read()
      #filename = os.path.basename(file)
      #self.result.append(filename)
      pathname = os.path.relpath(file, 'uploads')
      application_logger.debug(f"Filename: {pathname}, {file}")
      blob = self.repo.create_git_blob(data, "utf-8")
      element = github.InputGitTreeElement(path=pathname, mode='100644', type='blob', sha=blob.sha)
      self.elements.append(element)
      self.file_count += 1
      self.file_index += 1
      self.file_remaining_count -= 1
      return self.file_remaining_count > 0    
    except Exception as e:
      application_logger.exception(f"Exception raised while processing next file for Github", e)
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
      application_logger.exception(f"Exception raised while uploading to Github", e)
      raise self.UploadFail    

  def upload_file_list(self, dir):
    return [{'filename': x, 'file_path': os.path.join(self.project_root, dir, x)} for x in self.short_filenames]
  
  def progress(self):
    return self.file_count