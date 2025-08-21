import io

import google.auth
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.credentials import Credentials
SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]


def download_file(service, real_file_id):
  try:
    file_id = real_file_id

    request = service.files().get_media(fileId=file_id)
    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while done is False:
      status, done = downloader.next_chunk()
      print(f"Download {int(status.progress() * 100)}.")

  except HttpError as error:
    print(f"An error occurred: {error}")
    file = None

  return file.getvalue()


if __name__ == "__main__":
  download_file(real_file_id="1KuPmvGq8yoYgbfW74OENMCB5H0n_2Jm9")