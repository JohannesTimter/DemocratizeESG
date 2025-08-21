import os.path
import io

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from CompanyReportFile import Topic

from CompanyReportFile import CompanyReportFile
from Gemini import summarizeDoc

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
ca100_folder = '1ysF7PHBu29_0LGV-c22iwBoPx8X_NS9N' #ClimateActive100 Drive Folder


def main():
  creds = createCreds()

  companyReportFiles: list[CompanyReportFile] = []

  try:
    service = build("drive", "v3", credentials=creds)

    company_files = getFilesInFolder(service, ca100_folder)
    for company_folder in company_files:
      #print(f"{item['name']} ({item['id']}), View_URL: {item.get('webViewLink')}, Contentlink: {item.get('webContentLink')}")
      if company_folder['name'] == "Automobiles":
        auto_files = getFilesInFolder(service, company_folder['id'])

        for auto_file in auto_files:
          if auto_file['name'] == "MercedesBenz":
            merc_folder = getFilesInFolder(service, auto_file['id'])

            for merc_file in merc_folder:
              print(f"{merc_file['name']} ({merc_file['id']}), View_URL: {merc_file.get('webViewLink')}, Contentlink: {merc_file.get('webContentLink')}")
              if merc_file['mimeType'] == 'application/vnd.google-apps.folder':
                if "ESG" in merc_file['name']:
                  esg_files = getFilesInFolder(service, merc_file['id'])
                  for esg_file in esg_files:
                    if "2020" in esg_file['name']:
                      esg_file_2020 = CompanyReportFile(auto_file['name'], 2020, Topic.ESG, esg_file['mimeType'], download_file(service, esg_file['id']))
                      companyReportFiles.append(esg_file_2020)
                if "Financial" in merc_file['name']:
                  financial_files = getFilesInFolder(service, merc_file['id'])
                  for financial_file in financial_files:
                    if "2020" in financial_file['name']:
                      financial_file_2020 = CompanyReportFile(auto_file['name'], 2020, Topic.FINANCIAL, financial_file['mimeType'], download_file(service, financial_file['id']))
                      companyReportFiles.append(financial_file_2020)
                if "Annual Reports" in merc_file['name']:
                  annualReport_files = getFilesInFolder(service, merc_file['id'])
                  for annualReport_file in annualReport_files:
                    if "2020" in annualReport_file['name']:
                      annualReport_file = CompanyReportFile(auto_file['name'], 2020, Topic.ANNUAL_REPORT, annualReport_file['mimeType'], download_file(service, annualReport_file['id']))
                      companyReportFiles.append(annualReport_file)
              elif "2020" in merc_file['name']:
                annualReport_file = CompanyReportFile(auto_file['name'], 2020, Topic.ANNUAL_REPORT, merc_file['mimeType'], download_file(service, merc_file['id']))
                companyReportFiles.append(annualReport_file)

            print(f"reports retrieved: {len(companyReportFiles)}")

            summarizeDoc(companyReportFiles)



  except HttpError as error:
    # TODO(developer) - Handle errors from drive API.
    print(f"An error occurred: {error}")


def getFilesInFolder(service, folder_id):
  # Call the Drive v3 API
  query = f"'{folder_id}' in parents"
  results = (
    service.files()
    .list(q=query, pageSize=100, fields="nextPageToken, files(id, name, mimeType, webViewLink, webContentLink)")
    .execute()
  )
  items = results.get("files", [])

  if not items:
    print("No files found.")
    return
  sorted_items = sorted(items, key=lambda item: item['name'].lower())
  return sorted_items

def createCreds():
  creds = None
  if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
        "credentials.json", SCOPES
      )
      creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open("token.json", "w") as token:
      token.write(creds.to_json())
  return creds

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
  main()