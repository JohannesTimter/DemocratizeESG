import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]


def main():
  """Shows basic usage of the Drive v3 API.
  Prints the names and ids of the first 10 files the user has access to.
  """
  creds = Credentials.from_authorized_user_file("token.json", SCOPES)

  try:
    service = build("drive", "v3", credentials=creds)

    # Call the Drive v3 API
    results = (
        service.files()
        .list(pageSize=10, fields="nextPageToken, files(id, name, webViewLink, webContentLink)")
        .execute()
    )
    items = results.get("files", [])

    if not items:
      print("No files found.")
      return
    print("Files:")
    for item in items:
      print(f"{item['name']} ({item['id']}), View_URL: {item.get('webViewLink')}, Contentlink: {item.get('webContentLink')}")
  except HttpError as error:
    # TODO(developer) - Handle errors from drive API.
    print(f"An error occurred: {error}")


if __name__ == "__main__":
  main()