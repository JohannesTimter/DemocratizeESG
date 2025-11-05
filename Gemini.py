import asyncio
from io import BytesIO
import json
import math
import time
from pathlib import Path

import httpx
import pypdf
from google import genai
from google.genai import types
from google.genai.types import GenerateContentConfig

from GroundTruth import loadSheet
from pydantic import BaseModel

import CompanyReportFile
from MySQL_client import insertIntoMetricExtraction, selectDisclosedIndicatorIDs, select_communication_units

max_retries = 5
initial_delay_seconds = 5

class IndicatorExtraction(BaseModel):
  isDisclosed: int = 1
  indicator_id: str
  value: str
  unit: str
  page_number: str
  section: str

class CommunicationUnit(BaseModel):
  contains_information: int
  information: str
  page_number: str
  section: str

client = genai.Client()

async def promptDocumentsAsync(documents: list[CompanyReportFile]):



  for doc in documents:
    print(f"Now prompting document: {doc.company_name} {doc.period} {doc.topic} {doc.mimetype} {doc.counter} ")

    uploaded_docs_dict = handle_file_upload(doc)
    # uploaded_doc = uploadDoc(doc)

    for uploaded_doc_name in uploaded_docs_dict:
      #prompts = generatePromptsDictionary(doc)

    #doc_io = BytesIO(doc.file_value)
    #uploaded_doc = await client.aio.files.upload(
    #  file=doc_io,
    #  config=dict(
    #    mime_type=doc.mimetype)
    #)

      prompts = generatePromptsDictionary(doc)
      tasks = []
      for indicatorID in prompts:
        task = getGeminiResponseAsync(uploaded_docs_dict[uploaded_doc_name], prompts[indicatorID], indicatorID)
        tasks.append(task)

      results = await asyncio.gather(*tasks)

      for result in results:
        response = result[0]
        elapsed_time = result[1]
        indicatorID = result[2]
        thoughts = ""
        for part in response.candidates[0].content.parts:
          if not part.text:
            continue
          if part.thought:
            thoughts = part.text
        response_metadata = response.usage_metadata
        parsed_indicator: IndicatorExtraction = response.parsed
        parsed_indicator.indicator_id = indicatorID

        insertIntoMetricExtraction(doc, parsed_indicator, response_metadata, thoughts, elapsed_time)

    #client.files.delete(name=uploaded_doc.name)

async def getGeminiResponseAsync(uploaded_chunk, prompt, indicatorID):
  response = None
  start = 0
  for attempt in range(max_retries):
    start = time.time()
    try:
      async with asyncio.timeout(180):
        response = await client.aio.models.generate_content(
          model="gemini-2.5-flash",
          contents=[
            uploaded_chunk,
            prompt],
          config=GenerateContentConfig(
            thinking_config=types.ThinkingConfig(
              include_thoughts=True
            ),
            response_mime_type="application/json",
            response_schema=IndicatorExtraction
          )
        )

        #print(f"Parsed response: {response.parsed}")
    except (httpx.RemoteProtocolError, genai.errors.ServerError, asyncio.TimeoutError) as e:
      print(f"Caught an error: {e}")
      if attempt < max_retries - 1:
        delay = initial_delay_seconds * (2 ** attempt)
        print(f"Retrying in {delay} seconds...")
        time.sleep(delay)
      else:
        print("Max retries reached. The API call has failed.")
    except genai.errors.ClientError as e:
      if e.code == 429:
        if attempt < max_retries - 1:
          await asyncio.sleep(60)
      else:
        print(e)

  end = time.time()
  elapsed_time = int(end - start)
  print(f"IndicatorID: {indicatorID}, elapsed time: {elapsed_time} s")
  return response, elapsed_time, indicatorID, uploaded_chunk


def handle_file_upload(doc: CompanyReportFile):
  max_file_size = 30000000
  uploaded_chunks_dict = {}

  if int(doc.file_size) < max_file_size:
    uploaded_doc = uploadDoc(doc)
    doc_name = f"{doc.company_name}-{doc.period}-{doc.topic.name}-{doc.counter}"
    uploaded_chunks_dict[doc_name] = uploaded_doc
  else:
    parts_required = math.ceil(int(doc.file_size) / max_file_size)
    uploaded_chunks_dict = split_upload_pdf(doc, parts_required)

  return uploaded_chunks_dict

def split_upload_pdf(doc: CompanyReportFile, n_parts):
  uploaded_docs = {}
  pdf_stream = BytesIO(doc.file_value)
  pdf_reader = pypdf.PdfReader(pdf_stream)
  pdf_chunk_bytes_io = BytesIO()
  total_pages = len(pdf_reader.pages)
  #output_dir = Path("split_pdfs")
  #output_dir.mkdir(exist_ok=True)
  base_chunk_size = total_pages // n_parts

  start_index = 0
  # Loop through and create the first n-1 chunks
  for i in range(n_parts - 1):
    end_index = start_index + base_chunk_size
    pdf_writer = pypdf.PdfWriter()

    start = time.time()
    for page_num in range(start_index, end_index):
      pdf_writer.add_page(pdf_reader.pages[page_num])
      #pdf_reader.pages[page_num].compress_content_streams()
    end = time.time()
    print(f"time to add pages file: {int(end - start)}")

    #start = time.time()
    output_filename = f"{doc.company_name}-{doc.period}-{doc.topic.name}-{doc.counter}_chunk_{i + 1}_pages_{start_index + 1}_{end_index}"
    #output_path = output_dir / (output_filename + '.pdf')
    #with open(output_path, "wb") as output_file:
    #  pdf_writer.write(output_file)
    #end = time.time()
    #print(f"time to write file: {int(end - start)}")

    pdf_writer.write(pdf_chunk_bytes_io)
    uploaded_docs[output_filename] = upload_chunk(pdf_chunk_bytes_io)
    # uploaded_doc = upload_chunk(pdf_chunk_bytes_io)
    # uploaded_docs[output_filename] = UploadedChunk(i + 1, start_index + 1, end_index, uploaded_doc)
    # uploaded_docs[output_filename] = uploaded_doc

    print(
      f"Created '{output_filename}' with {end_index - start_index} pages. Uploaded with id {uploaded_docs[output_filename].name}")

    # Set the start index for the NEXT chunk (this creates the overlap)
    start_index = end_index - 1

  # 4. Create the final chunk with all remaining pages
  if start_index < total_pages:
    final_writer = pypdf.PdfWriter()
    # The last chunk goes from the last start_index all the way to the end
    for page_num in range(start_index, total_pages):
      final_writer.add_page(pdf_reader.pages[page_num])

    output_filename = f"{doc.company_name}-{doc.period}-{doc.topic.name}-{doc.counter}_chunk_{n_parts}_pages_{start_index + 1}_{total_pages}"
    #output_path = output_dir / (output_filename + '.pdf')
    #with open(output_path, "wb") as output_file:
    #  final_writer.write(output_file)

    final_writer.write(pdf_chunk_bytes_io)
    uploaded_docs[output_filename] = upload_chunk(pdf_chunk_bytes_io)
    # uploaded_doc = upload_chunk(pdf_chunk_bytes_io)
    # uploaded_docs[output_filename] = UploadedChunk(n_parts, start_index + 1, total_pages, uploaded_doc)

    print(
      f"Created '{output_filename}' with {total_pages - start_index} pages. Uploaded with id {uploaded_docs[output_filename].name}")

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

def createBatchRequestJson(all_companyYearReports):
  requests_data = []
  for doc in all_companyYearReports:
    uploaded_docs_dict = handle_file_upload(doc)
    #uploaded_doc = uploadDoc(doc)

    for uploaded_doc_name in uploaded_docs_dict:
      prompts = generatePromptsDictionary(doc)

      for indicatorID in prompts:
        request = {
          "key": f"{uploaded_doc_name}-{indicatorID}",
          "request": {
            "contents": [{
              "parts": [
                {"text": prompts[indicatorID]},
                {"file_data": {"file_uri": uploaded_docs_dict[uploaded_doc_name].uri, "mime_type": uploaded_docs_dict[uploaded_doc_name].mime_type}}
              ]
            }],
            "generationConfig": {
              "thinking_config": {
                "include_thoughts": True,
                "thinking_budget": -1
              },
              "response_mime_type": "application/json",
              "response_json_schema": IndicatorExtraction.model_json_schema()
            }
          }
        }
        requests_data.append(request)

      json_file_path = 'batch_input_output_files/big_dataset_tofix.json'
      print(f"Writing JSONL file: {json_file_path}")
      with open(json_file_path, 'w') as f:
          for req in requests_data:
              f.write(json.dumps(req) + '\n')

def uploadDoc(doc: CompanyReportFile):
    doc_io = BytesIO(doc.file_value)
    uploaded_doc =  client.files.upload(
      file=doc_io,
      config=dict(
        mime_type=doc.mimetype)
    )

    print(f"Uploaded doc: {doc.company_name} {doc.period} {doc.topic} {doc.counter}")
    return uploaded_doc

def promptDocuments(documents: list[CompanyReportFile]):

  for doc in documents:
    prompts = generatePromptsDictionary(doc)

    for indicatorID in prompts:
      print(f"Name: {doc.company_name}, Period: {doc.period}, Type: {doc.mimetype}, Topic: {doc.topic}")

      start = time.time()
      response = getGeminiResponse(doc, prompts[indicatorID])
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
      parsed_indicator: IndicatorExtraction = response.parsed
      parsed_indicator.indicator_id = indicatorID

      insertIntoMetricExtraction(doc, parsed_indicator, response_metadata, thoughts, elapsed_time)

def getGeminiResponse(doc, prompt):
  response = None
  for attempt in range(max_retries):
    try:
      response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=[
          types.Part.from_bytes(
            data=doc.file_value,
            mime_type=doc.mimetype,
          ),
          prompt],
        config=GenerateContentConfig(
          thinking_config=types.ThinkingConfig(
            include_thoughts=True
          ),
          response_mime_type="application/json",
          response_schema=IndicatorExtraction
        )
      )
    except (httpx.RemoteProtocolError, genai.errors.ServerError, genai.errors.ClientError) as e:
      print(f"Caught a remote protocol error: {e}")
      if attempt < max_retries - 1:
        delay = initial_delay_seconds * (2 ** attempt)
        print(f"Retrying in {delay} seconds...")
        time.sleep(delay)
      else:
        print("Max retries reached. The API call has failed.")
  return response

def get_communication_units(indicator_id, company_name, period):
  communication_units = select_communication_units(indicator_id, company_name, period)

  for communication_unit in communication_units:
    communication_unit = list(communication_unit)
    page_number = communication_unit[8]
    page_number_start = communication_unit[4]

    page_numbers_formatted = []
    page_number = page_number.replace("-", ",")
    page_numbers = page_number.split(',')
    for page_number in page_numbers:
      page_number = int(page_number)
      if page_number < page_number_start:
        page_number += page_number_start
      page_numbers_formatted.append(str(page_number))

    communication_unit[8] = ",".join(page_numbers)

  return communication_units

def build_c_u_string(communication_units):
  string = "Previous agents have already found the following relevant parts in the document:\n"

  for communication_unit in communication_units:
    string += "--------\n"
    string += f"Information: {communication_unit[7]}\n"
    string += f"Page number: {communication_unit[8]}\n"
    string += f"-------\n"

  return string

def promptTemplateCoA(indicatorInfos, doc):

  communication_units = get_communication_units(indicatorInfos['IndicatorID'], doc.company_name, doc.period)
  communication_units_string = build_c_u_string(communication_units)

  prompt = f"""You are an expert environmental data analyst. Your task is to extract information about the following metric from the attached report document:
      {indicatorInfos['IndicatorName']} of the reporting company {doc.company_name} for the year {doc.period}.

      Metric-specific instructions:
      {indicatorInfos['IndicatorDescription']}
      {indicatorInfos['PromptEngineering']}

      Suggested search words (you should still come up with your own search terms):
      {indicatorInfos['Searchwords']}

      {communication_units_string}

      Response Requirements:
      -You are only allowed to return a single json object in your response, you can never return multiple results.
      -Simple return the value and the unit as you find them. Do NOT perform unit conversion.     
      Example Output:
      {{            
            "is_disclosed": 1, //If the Information we are looking for is not disclosed in the document, set the is_disclosed field to 0.
            "indicator_id": "{indicatorInfos['IndicatorID']}":,
            "value": "{indicatorInfos['exampleValue']}", //
            "unit": "{indicatorInfos['exampleUnit']}", //Original unit, as stated in the source
            "page_number": "92", //page number, where the respective information was found.
            "section": "{indicatorInfos['exampleSourceSection']}" //text section where you found the information.
      }}

      General instructions:
      -Use the provided document as a source of metrics
      -Only consider english text
      -You are much better at reading tables and text than at interpreting figures. Knowing this, you prefer reading information from tables and text over using figures, if possible.
      -Hint: Look for tables in the Appendix and Annexes section of the reports, which can often be found in the last chapter of the documents. Look for tables in sections such as GRI indicators, SASB Indicators, TCFD Indicators. These tables contain reliable and easy to digest information.
      -Prefer values in metric tons over values the american short tons
      -Prefer values in liters over values in gallons
      -If there are values for the reporting company itself and for the reporting companies group available, use the values of the companies group.
      """

  return prompt



def generatePromptsDictionary(doc: CompanyReportFile):
  prompts = {}

  indicators = loadSheet("1QoOHmD0nxb52BIVpKyniVdYej1W5o1-sNot7DpaBl2w", "IndustryAgnostricIndicators!A1:J")

  alreadyDisclosedIndicators = selectDisclosedIndicatorIDs(doc)

  if doc.topic == CompanyReportFile.Topic.FINANCIAL:
    finance_indicators = ["lowCarbon_revenue", "environmental_ex", "revenue", "profit", "employees"]
    indicators = indicators.loc[indicators['IndicatorID'].isin(finance_indicators)]

  for index, row in indicators.iterrows():
    #prompts[row['IndicatorID']] = promptTemplate(row)
    #prompts[row['IndicatorID']] = promptTemplateCoA(row, doc)
    prompts[row['IndicatorID']] = promptTemplate2(row, doc)

  industry_specific_indicators = loadSheet("1QoOHmD0nxb52BIVpKyniVdYej1W5o1-sNot7DpaBl2w", "IndustrySpecificIndicators!A1:J")
  industry_specific_indicators = industry_specific_indicators.loc[industry_specific_indicators['Industry'] ==  doc.industry]
  for index, row in industry_specific_indicators.iterrows():
    #prompts[row['IndicatorID']] = promptTemplate(row)
    #prompts[row['IndicatorID']] = promptTemplateCoA(row, doc)
    prompts[row['IndicatorID']] = promptTemplate2(row, doc)

  #print(f"Following indicators are already disclosed and will not be prompted again: {",".join(alreadyDisclosedIndicators)}")

  return prompts

def promptTemplate(indicatorInfos):
  prompt = f""""Extract the following Information from the provided document:
      -{indicatorInfos['IndicatorName']}

      Provide the page number, where the respective information was found. 
      Also provide the text section where you found the information.
      If the Information we are looking for is not disclosed in the document, set the is_disclosed field to 0.
      The required output format is JSON.
      Example:
      {{            
            "is_disclosed": 1, 
            "indicator_id": "{indicatorInfos['IndicatorID']}":,
            "value": "{indicatorInfos['exampleValue']}",
            "unit": "{indicatorInfos['exampleUnit']}",
            "page_number": "92",
            "section" : "{indicatorInfos['exampleSourceSection']}"
      }}"""

  return prompt
  #print(prompt)

def promptTemplate2(indicatorInfos, doc):
  prompt = f""""You are an expert environmental data analyst. Your task is to extract the following metric from the attached report document:
      {indicatorInfos['IndicatorName']} of the reporting company {doc.company_name} for the year {doc.period}.
      
      Metric-specific instructions:
      {indicatorInfos['IndicatorDescription']}
      {indicatorInfos['PromptEngineering']}
      
      Suggested search words (you should still come up with your own search terms):
      {indicatorInfos['Searchwords']}
      
      Response Requirements:
      -You are only allowed to return a single json object in your response, you can never return multiple results.
      -Try to return the value in the unit as you find them. No need to perform unit conversion.     
      Example Output:
      {{            
            "is_disclosed": 1, //If the Information we are looking for is not disclosed in the document, set the is_disclosed field to 0.
            "indicator_id": "{indicatorInfos['IndicatorID']}",
            "value": "{indicatorInfos['exampleValue']}",
            "unit": "{indicatorInfos['exampleUnit']}", //Original unit, as stated in the source
            "page_number": "92", //page number, where the respective information was found.
            "section": "{indicatorInfos['exampleSourceSection']}" //text section where you found the information.
      }}

      General instructions:
      -Use the provided document as a source of metrics
      -Only consider english text
      -You are much better at reading tables and text than at interpreting figures. Knowing this, you prefer reading information from tables and text over using figures, if possible.
      -Hint: Look for tables in the Appendix and Annexes section of the reports, which can often be found in the last chapter of the documents. Look for tables in sections such as GRI indicators, SASB Indicators, TCFD Indicators. These tables contain reliable and easy to digest information.
      -Prefer values in metric tons over values the american short tons
      -Prefer values in liters over values in gallons
      -If there are values for the reporting company itself and for the reporting companies group available, use the values of the companies group.
      """

  return prompt

if __name__ == "__main__":
  prompts = generatePromptsDictionary()
  for prompt in prompts:
    print(prompt)
    print("----")