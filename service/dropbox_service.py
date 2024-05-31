import os
import time
import dropbox
from concurrent.futures import ThreadPoolExecutor
from service.upload_service import UploadService
from d4kms_generic import application_logger

class DropboxService(UploadService):

  MB = 1024 * 1024
  CHUNK_SIZE = 25165824
  TARGET = '4444'

  def __init__(self):
    super().__init__()
    self.client = dropbox.Dropbox(oauth2_access_token=self.service_environment.get('DROPBOX_TOKEN'))
    
  def upload(self):
    futures = []
    entries = []

    assert len(entries) <= 1000, "Max batch size is 1000."
    assert self.CHUNK_SIZE % (4 * self.MB) == 0, "Chunk size must be a multiple of 4 MB to use concurrent upload sessions"

    application_logger.info(f"Starting batch of {str(len(self.files))} upload sessions.")
    start_batch_result = self.client.files_upload_session_start_batch(
        num_sessions=len(self.files),
        session_type=dropbox.files.UploadSessionType.concurrent)

    with ThreadPoolExecutor(max_workers=20) as batch_executor:
        for index, file in enumerate(self.files):
            futures.append(
                batch_executor.submit(
                    self._prepare,
                    start_batch_result.session_ids[index], file
                )
            )

    for future in futures:
        entries.append(future.result())

    application_logger.info(f"Finishing batch of {str(len(entries))} entries.")
    finish_launch = self.client.files_upload_session_finish_batch(entries=entries)

    if finish_launch.is_async_job_id():
        application_logger.info(f"Polling for status of batch of {str(len(entries))} entries...")
        while True:
            finish_job = self.client.files_upload_session_finish_batch_check(
                async_job_id=finish_launch.get_async_job_id())
            if finish_job.is_in_progress():
                time.sleep(.5)
            else:
                complete = finish_job.get_complete()
                break
    if finish_launch.is_complete():
        complete = finish_launch.get_complete()
    elif finish_launch.is_other():
        raise Exception("Unknown finish result type!")

    application_logger.info(f"Finished batch of {str(len(entries))} entries.")

    for index, entry in enumerate(complete.entries):
        if entry.is_success():
          application_logger.info(f"File successfully uploaded to '{entry.get_success().path_lower}'.")
        elif entry.is_failure():
          application_logger.error(f"Commit for path '{entries[index].commit.path}' failed due to: {entry.get_failure()}")

    return

  def link(self, path, filename):
    try:
      link = self.client.files_get_temporary_link(f"{path}/{filename}")
      application_logger.info(f"Link for '{path}' and '{filename}' is '{link}'")
    except dropbox.exceptions.HttpError as err:
      application_logger.error(f"Failed to find link for '{path}' and '{filename}'")
      return None
    return link

  def _prepare(self, session_id, source_file_path):
    futures = []
    dest_file_name = os.path.basename(source_file_path)
    dest_folder = f'Uploads/{self.TARGET}'

    application_logger.info(f"Using upload session with ID '{session_id}' for file '{dest_file_name}'.")

    with open(source_file_path, "rb") as local_file:

      file_size = os.path.getsize(source_file_path)

      def append(dest_file_name, data, cursor, close):
          self.client.files_upload_session_append_v2(f=data, cursor=cursor, close=close)

      if file_size > 0:  # For non-empty files, start a number of concurrent append calls.
          with ThreadPoolExecutor(
              max_workers=10
          ) as session_executor:
              while local_file.tell() < file_size:
                  cursor = dropbox.files.UploadSessionCursor(session_id=session_id, offset=local_file.tell())
                  data = local_file.read(self.CHUNK_SIZE)
                  close = local_file.tell() == file_size
                  futures.append(session_executor.submit(append, dest_file_name, data, cursor, close))
      else:  # For empty files, just call append once to close the upload session.
        cursor = dropbox.files.UploadSessionCursor(session_id=session_id, offset=0)
        append(dest_file_name=dest_file_name, data=None, cursor=cursor, close=True)

      for future in futures:
        try:
          future.result()
        except Exception as append_exception:
          application_logger.error(f"Upload session with ID '{cursor.session_id}' failed.")
          raise append_exception

      return dropbox.files.UploadSessionFinishArg(
        cursor=dropbox.files.UploadSessionCursor(session_id=session_id, offset=local_file.tell()),
        commit=dropbox.files.CommitInfo(path=f"/{dest_folder}/{dest_file_name}")
      )

