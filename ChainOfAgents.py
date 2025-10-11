import json
import math, httpx, time, pypdf, pickle, sys
from io import BytesIO
from pathlib import Path
from pydantic import BaseModel

from CompanyReportFile import CompanyReportFile, Topic
from Fullcontext_main import retrieveCompanyYearReports, get_all_company_year_reports
from google import genai
from google.genai import types
from google.genai.types import GenerateContentConfig
import mysql.connector

from GroundTruth import loadSheet

sys.setrecursionlimit(6000) #pypdf runs into recursion problems with large pdfs
max_context = 10000
max_retries = 8
initial_delay_seconds = 5

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="MyN3wP4ssw0rd",
  database="democratizeesg"
)
mycursor = mydb.cursor()
client = genai.Client()

class CommunicationUnit(BaseModel):
  information: str
  page_number: str
  section: str

  def toString(self):
      return f"information: {self.information}\npage_number: {self.page_number}\nsection: {self.section}"

def selectAvgInputTokenCount(source_title: str):
    sql_query = ("SELECT truncate(avg(input_token_count),0) "
                 "FROM democratizeesg.extraction_attempt3_unconsolidated "
                 "WHERE source_title = %s")
    val = source_title,
    mycursor.execute(sql_query, val)
    results = mycursor.fetchall()

    avg_input_token_count = results[0][0]

    return avg_input_token_count

def split_upload_pdf(pdf_as_bytes, n_parts, company_name, year):
    uploaded_docs = {}
    pdf_stream = BytesIO(pdf_as_bytes)
    pdf_reader = pypdf.PdfReader(pdf_stream)
    pdf_chunk_bytes_io = BytesIO()
    total_pages = len(pdf_reader.pages)
    output_dir = Path("split_pdfs")
    output_dir.mkdir(exist_ok=True)
    base_chunk_size = total_pages // n_parts

    start_index = 0
    #Loop through and create the first n-1 chunks
    for i in range(n_parts - 1):
        end_index = start_index + base_chunk_size
        pdf_writer = pypdf.PdfWriter()

        start = time.time()
        for page_num in range(start_index, end_index):
            pdf_writer.add_page(pdf_reader.pages[page_num])
        end = time.time()
        print(f"time to add pages file: {int(end - start)}")

        start = time.time()
        output_filename = f"chunk_{i + 1}_pages_{start_index + 1}_{end_index}"
        output_path = output_dir / (output_filename +  '.pdf')
        with open(output_path, "wb") as output_file:
            pdf_writer.write(output_file)
        end = time.time()
        print(f"time to write file: {int(end - start)}")

        pdf_writer.write(pdf_chunk_bytes_io)
        uploaded_doc = upload_chunk(pdf_chunk_bytes_io)
        uploaded_docs[output_filename] = uploaded_doc

        print(f"Created '{output_filename}' with {end_index - start_index} pages. Uploaded with id {uploaded_doc.name}")

        #Set the start index for the NEXT chunk (this creates the overlap)
        start_index = end_index - 1

    # 4. Create the final chunk with all remaining pages
    if start_index < total_pages:
        final_writer = pypdf.PdfWriter()
        # The last chunk goes from the last start_index all the way to the end
        for page_num in range(start_index, total_pages):
            final_writer.add_page(pdf_reader.pages[page_num])



        output_filename = f"chunk_{n_parts}_pages_{start_index + 1}_{total_pages}"
        output_path = output_dir / (output_filename +  '.pdf')
        with open(output_path, "wb") as output_file:
            final_writer.write(output_file)

        final_writer.write(pdf_chunk_bytes_io)
        uploaded_doc = upload_chunk(pdf_chunk_bytes_io)
        uploaded_docs[output_filename] = uploaded_doc

        print(f"Created '{output_filename}' with {total_pages - start_index} pages. Uploaded with id {uploaded_doc.name}")

    return uploaded_docs

def upload_chunk(pdf_chunk_bytes_io):
    uploaded_doc = None
    start = time.time()

    retries = 0
    max_retries: int = 5
    initial_delay: float = 1.0
    backoff_factor: float = 2.0

    delay = initial_delay
    pdf_chunk_bytes_io.seek(0)
    try:
        uploaded_doc = client.files.upload(
            file=pdf_chunk_bytes_io,
            config=dict(
                mime_type="application/pdf")
        )
    except httpx.RemoteProtocolError as e:
        retries += 1
        if retries >= max_retries:
            print(f"Upload failed after {retries} retries.")
            raise e
        print(f"Upload failed with RemoteProtocolError. Retrying in {delay} seconds.")
        time.sleep(delay)
        delay *= backoff_factor

    end = time.time()
    print(f"time to upload pdf: {int(end - start)}")

    return uploaded_doc

def generateChainOfAgentsPrompt(indicatorsSheet, indicatorID, doc: CompanyReportFile, communicationUnits=[]):
    if doc.topic == CompanyReportFile.Topic.FINANCIAL:
        finance_indicators = ["lowCarbon_revenue", "environmental_ex", "revenue", "profit", "employees"]
        indicators = indicators.loc[indicators['IndicatorID'].isin(finance_indicators)]

    for index, row in indicators.iterrows():
        prompts[row['IndicatorID']] = promptTemplateCoA(row, doc)

    return prompts

def promptTemplateCoA(indicatorInfos, doc):

    prompt = f"""You are an expert environmental data analyst. Your task is to read the attached report document, then you should extract new information about the following metric: {indicatorInfos['IndicatorName']} of the reporting company {doc.company_name} for the year {doc.period}.
      Later, the information you collected will be used by another agent to find the value for {indicatorInfos['IndicatorName']}.

      #Metric-specific instructions:
      {indicatorInfos['IndicatorDescription']}
      {indicatorInfos['PromptEngineering']}

      #Suggested search words (you should still come up with your own search terms):
      {indicatorInfos['Searchwords']}

      #Response Requirements:
      -You are allowed to return a list of the following json object in your response
      -If you have found no relevant information in the attached report document, return an empty list.      
      Example Output:
      {{            
            "information": "I found a table in the 'OTHER ESG INFORMATION', with the subsection 'OTHER ENVIRONMENTAL INFORMATION'. The table is titled 'CO2e footprint'. A part of the table is labelled as 'SCOPE 1: DIRECT GREENHOUSE GAS EMISSIONS'. In the column for the year 2024 and the row 'Total emissions' I have found a value of 672,542, the unit is tCO2e.", //The relevant information you found in the attached report document. Give context!
            "page_number": "195", //page number (as visible on the page), where the respective information was found.
            "section": "OTHER ESG INFORMATION -> OTHER ENVIRONMENTAL INFORMATION -> CO2e footprint -> SCOPE 1: DIRECT GREENHOUSE GAS EMISSIONS -> Total emissions"//text section where you found the information.
      }}

      #General instructions:
      -Use the provided document as a source for information
      -Only consider english text
      -You are much better at reading tables and text than at interpreting figures. Knowing this, you prefer reading information from tables and text over using figures, if possible.
      -Hint: Look for tables in the Appendix and Annexes section of the reports, which can often be found in the last chapter of the documents. Look for tables in sections such as GRI indicators, SASB Indicators, TCFD Indicators. These tables contain reliable and easy to digest information.
      -Prefer values in metric tons over values the american short tons
      -Prefer values in liters over values in gallons
      -If there are values for the reporting company itself and for the reporting companies group available, use the values of the companies group.
      """

    return prompt

def getGeminiResponse(uploaded_doc_id, prompt):

    start = time.time()
    response = sendGeminiRequest(uploaded_doc_id, prompt)
    end = time.time()
    elapsed_time = int(end - start)
    print(response.text)
    print(f"Elapsed time: {elapsed_time} s")
    #print(response.usage_metadata)
    #print(f"Cached Tokens: {response.usage_metadata.cached_content_token_count}, Total Token: {response.usage_metadata.total_token_count}")

    thoughts = ""
    for part in response.candidates[0].content.parts:
        if not part.text:
          continue
        if part.thought:
          thoughts = part.text
          #print(f"Thought summary: {thoughts}")

    print("---")
    response_metadata = response.usage_metadata
    parsed_communication_unit: CommunicationUnit = response.parsed

    return parsed_communication_unit, thoughts, response_metadata

def sendGeminiRequest(uploaded_doc_id, prompt):
    response = None
    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=[
                    uploaded_doc_id,
                    prompt],
                config=GenerateContentConfig(
                    thinking_config=types.ThinkingConfig(
                        include_thoughts=True
                    ),
                    response_mime_type="application/json",
                    response_schema=list[CommunicationUnit]
                )
            )
        except (httpx.RemoteProtocolError, genai.errors.ServerError, genai.errors.ClientError) as e:
            print(f"Caught a remote protocol error: {e}")
            print(f"Prompt: {prompt}")
            if attempt < max_retries - 1:
                delay = initial_delay_seconds * (2 ** attempt)
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Max retries reached. The API call has failed.")
    return response

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


def main():
    #companyYearReports = retrieveCompanyYearReports("Automobiles", "BMW", '2024')

    #companyYearReports = get_all_company_year_reports()
    companyYearReports = []

    with open('companyYearReports.pkl', 'rb') as f:
        # Load the object from the file
        companyYearReports = pickle.load(f)

    #with open('companyYearReports.pkl', 'wb') as file:
    #    # 3. Use pickle.dump() to write the object to the file
    #    pickle.dump(companyYearReports, file)

    indicators_sheet = loadSheet("1QoOHmD0nxb52BIVpKyniVdYej1W5o1-sNot7DpaBl2w", "IndustryAgnostricIndicators!A1:J")
    requests_data = []
    json_file_path = 'batch_input_output_files/batchProcessing_file_promptTemplateCoA.json'

    # Read the batch request JSON object
    with open(json_file_path, 'r') as file:
        requests_data = [json.loads(line) for line in file]

    alreadyDone = ["QuantasAirways", "DeltaAirlines", "UnitedAirlines", "Volkswagen", "SAIC", "Toyota", "BMW", "MercedesBenz", "Cemex", "Holcim", "Ultratech",
                   "AirLiquide", "Dow", "Bayer", "ChinaShenhuaEnergy", "Walmart", "Nestle", "Panasonic", "BungeGlobal", "Wesfarmers", "BHP", "AnekaTambang"]

    for companyYearReport in companyYearReports:
        if companyYearReport.company_name in alreadyDone:
            continue
        source_title = f"{companyYearReport.company_name}_{companyYearReport.period}_{companyYearReport.topic.name}_{companyYearReport.counter}"
        avg_input_token_count = selectAvgInputTokenCount(source_title)

        print(f"{source_title}, {avg_input_token_count}")
        parts_required = math.ceil(avg_input_token_count / max_context)
        print(f"parts_required: {parts_required}")

        uploaded_chunks_dict = split_upload_pdf(companyYearReport.file_value, parts_required, companyYearReport.company_name, companyYearReport.period)

        #Pro Indicator: Einmal über alle docs drüber rutschen.
        for index, indicators_row in indicators_sheet.iterrows():
            if companyYearReport.topic == Topic.FINANCIAL:
                if indicators_row['IndicatorID'] not in ["lowCarbon_revenue", "environmental_ex", "revenue", "profit", "employees"]:
                    continue

            prompt = promptTemplateCoA(indicators_row, companyYearReport)
            for uploaded_chunk_name in uploaded_chunks_dict:
                request_data = createBatchRequestJson(companyYearReport, uploaded_chunk_name, uploaded_chunks_dict[uploaded_chunk_name], indicators_row['IndicatorID'], prompt)
                requests_data.append(request_data)

        print(f"\nCreating JSONL file: {json_file_path}")
        with open(json_file_path, 'w') as f:
            for req in requests_data:
                f.write(json.dumps(req) + '\n')


if __name__ == "__main__":
    main()
