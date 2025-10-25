import asyncio
import io
import pickle
import socket
import time

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from CompanyReportFile import Topic

from CompanyReportFile import CompanyReportFile
from Gemini import promptDocumentsAsync, uploadDoc, createBatchRequestJson
from GroundTruth import loadSheet

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
ca100_folder = '1ysF7PHBu29_0LGV-c22iwBoPx8X_NS9N' #ClimateActive100 Drive Folder
groundtruth_sheet_id = '18HCMbUmXcK9N2d4GziwUrHUgEHnc81ZH_4v-r-uJKcI' #Sheet with list of groundtruth sheets
groundtruth_sheet_range = "GroundTruth!A1:F"
test_sheet_range = "Test!A1:F"
big_dataset_range = "BigDataset!A1:C"

topic_order = {
    Topic.ESG: 0,
    Topic.ANNUAL_REPORT: 1,
    Topic.FINANCIAL: 2,
}

async def main():
  #await fullcontext_async()

  all_companyYearReports = get_all_company_year_reports()
  createBatchRequestJson(all_companyYearReports)


def get_all_company_year_reports():
  all_companyYearReports = []

  with open('companyYearReports.pkl', 'rb') as f:
    #    # Load the object from the file
    all_companyYearReports = pickle.load(f)

  industries_to_collect = ["Airlines", "Automobiles", "Cement", "Chemicals", "CoalMining"]
  years_to_collect = ["2020", "2021", "2022", "2023", "2024"]

  groundtruth_reportsList = loadSheet(groundtruth_sheet_id, big_dataset_range)
  for index, row in groundtruth_reportsList.iterrows():
    if row['Industry'] in industries_to_collect and row['Collected'] != "TRUE":
      for year in years_to_collect:
        print(f"Now collecting documents: {row['Company']} {year}")
        companyYearReports = retrieveCompanyYearReports(row['Industry'], row['Company'], year)
        print(f"Retrieved {len(companyYearReports)} documents for {row['Company']} {year}")
        all_companyYearReports.extend(companyYearReports)

      with open('companyYearReports.pkl', 'wb') as file:
        # 3. Use pickle.dump() to write the object to the file
        pickle.dump(all_companyYearReports, file)
        print(f"companyYearReports.pkl aktualisiert.")

  return all_companyYearReports

async def fullcontext_async():
  groundtruth_reportsList = loadSheet(groundtruth_sheet_id, test_sheet_range)
  for index, row in groundtruth_reportsList.iterrows():
    if row['Collected'] == "TRUE":
      print(f"{row['Company']} already collected, skipping")
      continue

    start = time.time()
    companyYearReports = retrieveCompanyYearReports(row['Industry'], row['Company'], row['Year'])
    for companyYearReport in companyYearReports:
      print(
        f"CompanyName: {companyYearReport.company_name}, Topic: {companyYearReport.topic}, MimeType: {companyYearReport.mimetype}, Size: {companyYearReport.file_size}, Counter: {companyYearReport.counter}")
    # promptDocuments(companyYearReports)
    await promptDocumentsAsync(companyYearReports)
    end = time.time()
    print(f"Time elapsed for {row['Company']}: {int(end - start)}s")

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
  acceptable_mime_types = ['application/pdf']

  esgCounter, finCounter, annCounter = 0, 0, 0
  relevantCounter = None

  for company_report_file in company_report_files:
    if company_report_file['mimeType'] == 'application/vnd.google-apps.folder':
      if "ESG" in company_report_file['name']:
        topic = Topic.ESG
        relevantCounter = esgCounter
      elif "Financial" in company_report_file['name']:
        topic = Topic.FINANCIAL
        relevantCounter = finCounter
      else:
        topic = Topic.ANNUAL_REPORT
        relevantCounter = annCounter
      specific_company_report_files = getFilesInFolder(service, company_report_file['id'])
      for specific_company_report_file in specific_company_report_files:
        if year in specific_company_report_file['name']:
          if specific_company_report_file['mimeType'] in acceptable_mime_types:
            relevantCounter += 1
            companyReports.append(CompanyReportFile(industry, company, year, topic, specific_company_report_file['mimeType'],
                                                    download_file(service, specific_company_report_file), specific_company_report_file['size'], relevantCounter))
          else:
            print(f"Unexpected mime type {specific_company_report_file['mimeType']} for company {company} in year {year}")
    elif year in company_report_file['name']:
      if company_report_file['mimeType'] in acceptable_mime_types:
        annCounter += 1
        companyReports.append(
          CompanyReportFile(industry, company, year, Topic.ANNUAL_REPORT, company_report_file['mimeType'],
                            download_file(service, company_report_file), company_report_file['size'], annCounter))
      else:
        print(f"Unexpected mime type {company_report_file['mimeType']} for company {company} in year {year}")

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

def download_file(service, report_file):
  file_id = report_file['id']
  mimeType = report_file['mimeType']
  MAX_RETRIES = 5
  BASE_BACKOFF = 2  # Seconds
  retries = 0
  request = None

  try:
    if mimeType == 'application/pdf':
      request = service.files().get_media(fileId=file_id)
    elif mimeType == 'application/vnd.google-apps.spreadsheet':
      request = service.files().export_media(
          fileId=file_id, mimeType="application/pdf"
      )
    else:
      raise NotImplementedError(f"Unexpected mime type: {mimeType}, id: {file_id}")
    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while done is False:
      status, done = downloader.next_chunk()
      retries = 0
      #print(f"Download {int(status.progress() * 100)} of {report_file['name']} {mimeType}")

  except HttpError as error:
    print(f"An error occurred: {error}")
    file = None
  except (socket.timeout, TimeoutError) as e:
    print(f"A timeout error occurred: {e}")
    if retries < MAX_RETRIES:
      retries += 1
      sleep_time = (BASE_BACKOFF ** retries)
      print(f"Retrying in {sleep_time}s... (Attempt {retries}/{MAX_RETRIES})")
      time.sleep(sleep_time)
    else:
      print(f"Download failed after {MAX_RETRIES} retries due to timeout.")
      file = None  # Mark as failed

  return file.getvalue()


if __name__ == "__main__":
  asyncio.run(main())