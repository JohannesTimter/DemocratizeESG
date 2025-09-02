import io

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from CompanyReportFile import Topic

from CompanyReportFile import CompanyReportFile
from Gemini import promptDocuments

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
ca100_folder = '1ysF7PHBu29_0LGV-c22iwBoPx8X_NS9N' #ClimateActive100 Drive Folder

topic_order = {
    Topic.ESG: 0,
    Topic.ANNUAL_REPORT: 1,
    Topic.FINANCIAL: 2,
}

def main():
  companyYearReports = retrieveCompanyYearReports("Chemicals", "Dow", "2024")

  for companyYearReport in companyYearReports:
    print(f"CompanyName: {companyYearReport.company_name}, Topic: {companyYearReport.topic}, MimeType: {companyYearReport.mimetype}, Size: {companyYearReport.file_size}")

  promptDocuments(companyYearReports)

def retrieveCompanyYearReports(industry, companyName, year):
  creds = Credentials.from_authorized_user_file("token.json", SCOPES)

  try:
    service = build("drive", "v3", credentials=creds)
    industry_folders = getFilesInFolder(service, ca100_folder)
    for industry_folder in industry_folders:
      if industry_folder['name'] == industry:
        company_folders = getFilesInFolder(service, industry_folder['id'])
        for company_folder in company_folders:
          if company_folder['name'] == companyName:
            company_report_files = getFilesInFolder(service, company_folder['id'])
            companyReports = handleCompanyFiles(company_report_files, industry, companyName, service, year)
            sorted_companyReports = sorted(
              companyReports,
              key=lambda report: (topic_order.get(report.topic), report.file_size)
            )
            return sorted_companyReports
  except HttpError as error:
    print(f"An error occurred: {error}")

def handleCompanyFiles(company_report_files, industry, company, service, year):
  companyReports: list[CompanyReportFile] = []

  for company_report_file in company_report_files:
    if company_report_file['mimeType'] == 'application/vnd.google-apps.folder':
      if "ESG" in company_report_file['name']:
        topic = Topic.ESG
      elif "Financial" in company_report_file['name']:
        topic = Topic.FINANCIAL
      else:
        topic = Topic.ANNUAL_REPORT
      specific_company_report_files = getFilesInFolder(service, company_report_file['id'])
      for specific_company_report_file in specific_company_report_files:
        if year in specific_company_report_file['name']:
          companyReports.append(CompanyReportFile(industry, company, year, topic, specific_company_report_file['mimeType'],
                                                  download_file(service, specific_company_report_file['id']), specific_company_report_file['size']))
    elif year in company_report_file['name']:
      companyReports.append(
        CompanyReportFile(industry, company, year, Topic.ANNUAL_REPORT, company_report_file['mimeType'],
                          download_file(service, company_report_file['id']), company_report_file['size']))

  return companyReports

def getFilesInFolder(service, folder_id):
  # Call the Drive v3 API
  query = f"'{folder_id}' in parents"
  results = (
    service.files()
    .list(q=query, pageSize=100, fields="nextPageToken, files(id, name, mimeType, webViewLink, webContentLink, size)")
    .execute()
  )
  items = results.get("files", [])

  if not items:
    print("No files found.")
    return
  sorted_items = sorted(items, key=lambda item: item['name'].lower())
  return sorted_items

def download_file(service, file_id):
  try:
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