from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd

from MySQL_client import insertIntoGroundtruth
from createGoogleAccessToken import SCOPES

# The ID and range of a sample spreadsheet.
groundTruthReportlist_sheetID = "18HCMbUmXcK9N2d4GziwUrHUgEHnc81ZH_4v-r-uJKcI"
groundTruthReportlist_range = "GroundTruth!A1:D"
groundTruthReport_range = "Sheet1!A1:L"
creds = Credentials.from_authorized_user_file("token.json", SCOPES)


def main():
  groundtruthreports_df = loadSheet(groundTruthReportlist_sheetID, groundTruthReportlist_range)

  for index, overview_row in groundtruthreports_df.iterrows():
    print(f"Industry: {overview_row['Industry']}, Company:{overview_row['Company']}, Year: {overview_row['Year']}, GroundTruthLink: {overview_row['GroundTruth']}")
    groundtruthreport_sheetID = overview_row['GroundTruth'].split('/d/')[1].split('/edit?')[0]
    groundtruthreport_df = loadSheet(groundtruthreport_sheetID, groundTruthReport_range)

    for report_index, report_row in groundtruthreport_df.iterrows():
      insertIntoGroundtruth(overview_row, report_row)

def loadSheet(spreadsheet_id, range):
  """Shows basic usage of the Sheets API.
  Prints values from a sample spreadsheet.
  """
  try:
    service = build("sheets", "v4", credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = (
        sheet.values()
        .get(spreadsheetId=spreadsheet_id, range=range)
        .execute()
    )
    values = result.get("values", [])

    # 'values' is the list of lists you retrieved from the Google Sheets API
    if not values:
      print("No data found.")
      df = pd.DataFrame()  # Create an empty DataFrame if no data
    else:
      # Use the first row as the header and the rest as data
      headers = values[0]
      data = values[1:]
      df = pd.DataFrame(data, columns=headers)

    # Now you can work with your DataFrame
    return df
  except HttpError as err:
    print(err)


if __name__ == "__main__":
  main()