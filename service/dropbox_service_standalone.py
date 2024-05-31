from concurrent.futures import ThreadPoolExecutor
from d4kms_generic import ServiceEnvironment
from d4kms_generic import application_logger

import os
import time

import dropbox

MB = 1024 * 1024
CHUNK_SIZE = 25165824
TARGET = '4444'

def get_client():
    try:
      se = ServiceEnvironment()
      return dropbox.Dropbox(oauth2_access_token=se.get('DROPBOX_TOKEN'))
    except Exception as e:
      raise Exception("Either an access token or refresh token/app key is required.")

def collect_files(folder_path):
    """Returns the list of files to upload."""
    folder_path = os.path.expanduser(folder_path)
    # List all of the files inside the specified folder.
    files = sorted(
        [os.path.join(folder_path, f)
         for f in os.listdir(folder_path)
         if os.path.isfile(os.path.join(folder_path, f))  # ignores folders
         and f not in [".DS_Store", ".localized", ".gitignore"]  # ignores system files, etc.
         ]
    )
    application_logger.info(f"Collected {str(len(files))} files for upload")
    short_names = [os.path.basename(x) for x in files]
    #print(f"FILES: {short_names}")
    return files


def upload_session_appends(client, session_id, source_file_path):
    """Performs parallelized upload session appends for one file."""

    futures = []

    dest_file_name = os.path.basename(source_file_path)
    dest_folder = f'Uploads/{TARGET}'

    application_logger.info(f"Using upload session with ID '{session_id}' for file '{dest_file_name}'.")

    with open(source_file_path, "rb") as local_file:

        file_size = os.path.getsize(source_file_path)

        def append(dest_file_name, data, cursor, close):
            # application_logger.debug(f"Appending to upload session with ID '{cursor.session_id}'"
            #               f" for file '{dest_file_name}'"
            #               f" at offset: {str(cursor.offset)}")
            client.files_upload_session_append_v2(f=data,
                                                  cursor=cursor,
                                                  close=close)
            # application_logger.debug(f"Done appending to upload session with ID '{cursor.session_id}'"
            #               f" for file '{dest_file_name}'"
            #               f" at offset: {str(cursor.offset)}")

        if file_size > 0:  # For non-empty files, start a number of concurrent append calls.
            with ThreadPoolExecutor(
                max_workers=10
            ) as session_executor:
                while local_file.tell() < file_size:
                    cursor = dropbox.files.UploadSessionCursor(
                        session_id=session_id, offset=local_file.tell())
                    data = local_file.read(CHUNK_SIZE)
                    close = local_file.tell() == file_size
                    futures.append(
                        session_executor.submit(append, dest_file_name, data, cursor, close))
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
            cursor=dropbox.files.UploadSessionCursor(
                session_id=session_id, offset=local_file.tell()),
            commit=dropbox.files.CommitInfo(path=f"/{dest_folder}/{dest_file_name}"))


def upload_files(client, files):
    """Performs upload sessions for a batch of files in parallel."""

    futures = []
    entries = []
    uploaded_size = 0

    assert len(entries) <= 1000, "Max batch size is 1000."
    assert CHUNK_SIZE % (4 * MB) == 0, \
        "Chunk size must be a multiple of 4 MB to use concurrent upload sessions"

    application_logger.info(f"Starting batch of {str(len(files))} upload sessions.")
    start_batch_result = client.files_upload_session_start_batch(
        num_sessions=len(files),
        session_type=dropbox.files.UploadSessionType.concurrent)

    with ThreadPoolExecutor(max_workers=20) as batch_executor:
        for index, file in enumerate(files):
            futures.append(
                batch_executor.submit(
                    upload_session_appends,
                    client, start_batch_result.session_ids[index], file
                )
            )

    for future in futures:
        entries.append(future.result())
        #uploaded_size += future.result().cursor.offset

    application_logger.info(f"Finishing batch of {str(len(entries))} entries.")
    finish_launch = client.files_upload_session_finish_batch(entries=entries)

    if finish_launch.is_async_job_id():
        application_logger.info(f"Polling for status of batch of {str(len(entries))} entries...")
        while True:
            finish_job = client.files_upload_session_finish_batch_check(
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
            application_logger.error(f"Commit for path '{entries[index].commit.path}'"
                          f" failed due to: {entry.get_failure()}")

    return uploaded_size

def getTemporaryLink(dbx, path):
	try:
		link = dbx.files_get_temporary_link(path)
	except dropbox.exceptions.HttpError as err:
		print('*** HTTP error', err)
		return None
	return link

def run_and_time_uploads():
    client = get_client()
    files = collect_files(folder_path='Uploads/f2579b76-9c62-4fb4-983b-45e8cfd17176')

    #start_time = time.time()
    uploaded_size = upload_files(client=client, files=files)
    #end_time = time.time()

    #time_elapsed = end_time - start_time
    #application_logger.info(f"Uploaded {uploaded_size} bytes in {time_elapsed:.2f} seconds.")

    #megabytes_uploaded = uploaded_size / MB
    #application_logger.info(f"Approximate overall speed: {megabytes_uploaded / time_elapsed:.2f} MB/s.")

    x = getTemporaryLink(client, f'/Uploads/{TARGET}/node-address-1.csv')
    print(f"LINK: {x.link}")

def get_link():
  client = get_client()
  #x = getTemporaryLink(client, f'/Apps/StudyService/Uploads/{TARGET}/node-address-1.csv')
  #x = getTemporaryLink(client, f'/Uploads/{TARGET}/node-address-1.csv')
  x = getTemporaryLink(client, f'/Uploads/{TARGET}')
  print(f"LINK: {x.link}")

if __name__ == '__main__':
  run_and_time_uploads()
  #get_link()