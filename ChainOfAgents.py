import asyncio
import json
import pickle, sys
from pathlib import Path

from mysql.connector import IntegrityError

from CompanyReportFile import CompanyReportFile, Topic
from google import genai
import mysql.connector

from Fullcontext_main import get_all_company_year_reports
from Gemini import getGeminiResponseAsync, CommunicationUnit
from GroundTruth import loadSheet
from MySQL_client import createDocumentName
from utils import split_upload_pdf

sys.setrecursionlimit(6000) #pypdf runs into recursion problems with large pdfs -> increase recursion limit
max_context = 20000
max_retries = 8
initial_delay_seconds = 5

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="MyN3wP4ssw0rd", #local db, so no need for .env
  database="democratizeesg"
)
mycursor = mydb.cursor()
client = genai.Client()

class UploadedChunk:
    id: int
    page_start: int
    page_end: int
    uploaded_doc_reference: object

    def __init__(self, id, page_start, page_end, uploaded_doc_reference):
        self.id = id
        self.page_start = page_start
        self.page_end = page_end
        self.uploaded_doc_reference = uploaded_doc_reference

def promptTemplateCoA(indicatorInfos, doc):

    prompt = f"""You are an expert environmental data analyst. Your task is to read the attached report document, then you should extract new information about the following metric: {indicatorInfos['IndicatorName']} of the reporting company {doc.company_name} for the year {doc.period}.
      Later, the information you collected will be used by another agent to find the value for {indicatorInfos['IndicatorName']}.

      #Metric-specific instructions:
      {indicatorInfos['IndicatorDescription']}
      {indicatorInfos['PromptEngineering']}

      #Suggested search words (you should still come up with your own search terms):
      {indicatorInfos['Searchwords']}

      #Response Requirements:
      -If you have found relevant Information, set contains_information to 1
      -If you have found no relevant information in the attached report document, set contains_information to 0
      -If you find multiple, relevant information you can return multiple objects in the list      
      Example Output:
      [{{
            "contains_information": "1" //set to 0, if the document contains no relevant information            
            "information": "I found a table in the 'OTHER ESG INFORMATION', with the subsection 'OTHER ENVIRONMENTAL INFORMATION'. The table is titled 'CO2e footprint'. A part of the table is labelled as 'SCOPE 1: DIRECT GREENHOUSE GAS EMISSIONS'. In the column for the year 2024 and the row 'Total emissions' I have found a value of 672,542, the unit is tCO2e.", //The relevant information you found in the attached report document. Give context!
            "page_number": "195", //page number(s) as visible on the page(s), where the respective information was found.
            "section": "OTHER ESG INFORMATION -> OTHER ENVIRONMENTAL INFORMATION -> CO2e footprint -> SCOPE 1: DIRECT GREENHOUSE GAS EMISSIONS -> Total emissions"//text section where you found the information.
      }}]

      #General instructions:
      -Do not convert any units or values
      -Use the provided document as a source for information
      -Only consider english text
      -You are much better at reading tables and text than at interpreting figures. Knowing this, you prefer reading information from tables and text over using figures, if possible.
      -Hint: Look for tables in the Appendix and Annexes section of the reports, which can often be found in the last chapter of the documents. Look for tables in sections such as GRI indicators, SASB Indicators, TCFD Indicators. These tables contain reliable and easy to digest information.
      -Prefer values in metric tons over values the american short tons
      -Prefer values in liters over values in gallons
      -If there are values for the reporting company itself and for the reporting companies group available, use the values of the companies group.
      """

    return prompt

def createBatchRequestJson(company_year_report, uploaded_chunk_name, uploaded_chunk, indicator_id, prompt):
    request = {
        "key": f"{company_year_report.company_name}-{company_year_report.period}-{company_year_report.topic.name}-{company_year_report.counter}-{uploaded_chunk_name}-{indicator_id}",
        "request": {
            "contents": [{
                "parts": [
                    {"text": prompt},
                    {"file_data": {"file_uri": uploaded_chunk.uri, "mime_type": uploaded_chunk.mime_type}}
                ]
            }],
            "generationConfig": {
                "thinking_config": {
                    "include_thoughts": True,
                    "thinking_budget": -1
                },
                "response_mime_type": "application/json",
                "response_json_schema": CommunicationUnit.model_json_schema()
            }
        }
    }
    return request

def storeReportsLocally(companyYearReports, json_file_path):
    for report in companyYearReports:
        filename = f"batch_input_output_files/{report.company_name}_{report.period}_{report.topic}_{report.counter}.pdf"
        print(f"\nCreating JSON file: {filename}")
        with open(filename, 'wb') as f:
            f.write(report.file_value)

def insert_cu_into_table(doc: CompanyReportFile, communication_unit: CommunicationUnit, indicatorID, uploaded_chunk: UploadedChunk, response_metadata, thoughts, elapsed_time):
    sql = ("INSERT INTO communicationunits_test (company_name, year, chunk_id, chunk_page_start, "
           "chunk_page_end, indicator_id, information, pagenumber, source_title, text_section, cached_content_token_count, total_token_count, thought_summary, elapsed_time)"
           " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    val = (doc.company_name, doc.period, uploaded_chunk.id, uploaded_chunk.page_start, uploaded_chunk.page_end, indicatorID, communication_unit.information[:2999], communication_unit.page_number[:200],
           createDocumentName(doc), communication_unit.section[:2999], response_metadata.cached_content_token_count,
           response_metadata.total_token_count, thoughts[:4999], elapsed_time)
    mycursor.execute(sql, val)

    mydb.commit()

def handle_chunk_results(doc: CompanyReportFile, results):
    for result in results:
        response = result[0]
        elapsed_time = result[1]
        indicatorID = result[2]
        uploaded_chunk: UploadedChunk = result[3]
        thoughts = ""

        if response == None:
            print(f"Results: {results}")

        for part in response.candidates[0].content.parts:
            if not part.text:
                continue
            if part.thought:
                thoughts = part.text
        response_metadata = response.usage_metadata
        communication_units: list[CommunicationUnit] = response.parsed

        for communication_unit in communication_units:
            if communication_unit.contains_information == 1:
                insert_cu_into_table(doc, communication_unit, indicatorID, uploaded_chunk, response_metadata, thoughts, elapsed_time)

def load_or_generate_safepoint(filepath, generate_func, *args, **kwargs):
    path = Path(filepath)
    if path.exists():
        print(f"Loading cached state from {filepath}...")
        with path.open('rb') as f:
            return pickle.load(f)

    print(f"Generating new data for {filepath}...")
    data = generate_func(*args, **kwargs)

    with path.open('wb') as f:
        pickle.dump(data, f)

    return data

async def main():

    companyYearReports = load_or_generate_safepoint(
        'companyYearReports.pkl',
        get_all_company_year_reports
    )

    indicators_sheet = loadSheet("1QoOHmD0nxb52BIVpKyniVdYej1W5o1-sNot7DpaBl2w", "IndustryAgnostricIndicators!A1:J")

    alreadyDone = ["GeneralMotors", "CHR_plc", "BASF", "Carrefour", "RioTinto", "Iberdrola", "NextEra", "FormosaPetrochemical", "bp", "ExxonMobil"]

    for companyYearReport in companyYearReports:
        if companyYearReport.company_name in alreadyDone:
            continue

        uploaded_chunks_dict = load_or_generate_safepoint(
            'pickles/chunks.pkl',
            split_upload_pdf,
            companyYearReport
        )

        #Asynchronous, immediate processing:
        for uploaded_chunk in uploaded_chunks_dict:
            print(f"Prompting chunk: {uploaded_chunk}")
            tasks = []
            for index, indicators_row in indicators_sheet.iterrows():
                if companyYearReport.topic == Topic.FINANCIAL:
                    if indicators_row['IndicatorID'] not in ["lowCarbon_revenue", "environmental_ex", "revenue",
                                                             "profit", "employees"]:
                        continue
                prompt = promptTemplateCoA(indicators_row, companyYearReport)
                task = getGeminiResponseAsync(uploaded_chunks_dict[uploaded_chunk], prompt,indicators_row['IndicatorID'])
                tasks.append(task)
            results = await asyncio.gather(*tasks)
            handle_chunk_results(companyYearReport, results)

            client.files.delete(name=uploaded_chunks_dict[uploaded_chunk].uploaded_doc_reference.name)

        #In case of batch processing:
        #createRequestsDataBatchCoA(companyYearReport, indicators_sheet, requests_data, uploaded_chunks_dict)

def createRequestsDataBatchCoA(companyYearReport, indicators_sheet, requests_data, uploaded_chunks_dict):
    requests_data = []
    json_file_path = 'batch_input_output_files/12_companies_test_CoA.json'
    #Read the batch request JSON object
    with open(json_file_path, 'r') as file:
        requests_data = [json.loads(line) for line in file]

    # Pro Indicator: Einmal über alle chunks drüber rutschen.
    for index, indicators_row in indicators_sheet.iterrows():
        if companyYearReport.topic == Topic.FINANCIAL:
            if indicators_row['IndicatorID'] not in ["lowCarbon_revenue", "environmental_ex", "revenue", "profit",
                                                     "employees"]:
                continue

        prompt = promptTemplateCoA(indicators_row, companyYearReport)
        for uploaded_chunk_name in uploaded_chunks_dict:
            request_data = createBatchRequestJson(companyYearReport, uploaded_chunk_name,
                                                  uploaded_chunks_dict[uploaded_chunk_name],
                                                  indicators_row['IndicatorID'], prompt)
            requests_data.append(request_data)

    print(f"\nCreating JSONL file: {json_file_path}")
    with open(json_file_path, 'w') as f:
       for req in requests_data:
           f.write(json.dumps(req) + '\n')

if __name__ == "__main__":
    asyncio.run(main())