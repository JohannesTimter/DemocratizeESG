import asyncio
import io
import json
import time

import httpx
from google import genai
from google.genai import types
from google.genai.types import GenerateContentConfig

from GroundTruth import loadSheet
from pydantic import BaseModel

import CompanyReportFile
from MySQL_client import insertIntoMetricExtraction

max_retries = 8
initial_delay_seconds = 5

class IndicatorExtraction(BaseModel):
  isDisclosed: int = 1
  indicator_id: str
  value: str
  unit: str
  page_number: str
  section: str

client = genai.Client()

async def promptDocumentsAsync(documents: list[CompanyReportFile]):
  for doc in documents:
    print(f"Now prompting document: {doc.company_name} {doc.period} {doc.topic} {doc.mimetype} {doc.counter} ")

    doc_io = io.BytesIO(doc.file_value)
    uploaded_doc = await client.aio.files.upload(
      file=doc_io,
      config=dict(
        mime_type=doc.mimetype)
    )

    prompts = generatePromptsDictionary(doc)
    tasks = []
    for indicatorID in prompts:
      task = getGeminiResponseAsync(uploaded_doc, prompts, indicatorID)
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

    client.files.delete(name=uploaded_doc.name)

async def getGeminiResponseAsync(uploaded_doc, prompts, indicatorID):
  response = None
  start = 0
  for attempt in range(max_retries):
    start = time.time()
    try:
      response = await client.aio.models.generate_content(
        model="gemini-2.5-flash",
        contents=[
          uploaded_doc,
          prompts[indicatorID]],
        config=GenerateContentConfig(
          thinking_config=types.ThinkingConfig(
            include_thoughts=True
          ),
          response_mime_type="application/json",
          response_schema=IndicatorExtraction
        )
      )

    except (httpx.RemoteProtocolError, genai.errors.ServerError) as e:
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
  return response, elapsed_time, indicatorID

def createBatchRequestJson(all_companyYearReports):
  requests_data = []
  for doc in all_companyYearReports:
    uploaded_doc = uploadDoc(doc)
    prompts = generatePromptsDictionary(doc)

    for indicatorID in prompts:
      request = {
        "key": f"{doc.company_name}-{doc.period}-{doc.topic.name}-{doc.counter}-{indicatorID}",
        "request": {
          "contents": [{
            "parts": [
              {"text": prompts[indicatorID]},
              {"file_data": {"file_uri": uploaded_doc.uri, "mime_type": uploaded_doc.mime_type}}
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

  json_file_path = 'batchProcessing_file_promptTemplate3.json'
  print(f"\nCreating JSONL file: {json_file_path}")
  with open(json_file_path, 'w') as f:
      for req in requests_data:
          f.write(json.dumps(req) + '\n')

def uploadDoc(doc: CompanyReportFile):
    doc_io = io.BytesIO(doc.file_value)
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

def generatePromptsDictionary(doc: CompanyReportFile):
  prompts = {}

  indicators = loadSheet("1QoOHmD0nxb52BIVpKyniVdYej1W5o1-sNot7DpaBl2w", "IndustryAgnostricIndicators!A1:J")
  #alreadyDisclosedIndicators = selectDisclosedIndicatorIDs(doc)

  if doc.topic == CompanyReportFile.Topic.FINANCIAL:
    finance_indicators = ["lowCarbon_revenue", "environmental_ex", "revenue", "profit", "employees"]
    indicators = indicators.loc[indicators['IndicatorID'].isin(finance_indicators)]

  for index, row in indicators.iterrows():
    #if row['IndicatorID'] not in alreadyDisclosedIndicators:
      #prompts[row['IndicatorID']] = promptTemplate(row)
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
      -{indicatorInfos['IndicatorName']} of the reporting company {doc.company_name} for the year {doc.period}.
      
      Metric-specific instructions:
      {indicatorInfos['IndicatorDescription']}
      {indicatorInfos['PromptEngineering']}
      
      Suggested search words (you should still come up with your own searchwords):
      {indicatorInfos['Searchwords']}
      
      Response Requirements:
      -You are only allowed to return a single json object in your response, you can never return multiple results.      
      Example Output:
      {{            
            "is_disclosed": 1, //If the Information we are looking for is not disclosed in the document, set the is_disclosed field to 0.
            "indicator_id": "{indicatorInfos['IndicatorID']}":,
            "value": "{indicatorInfos['exampleValue']}",
            "unit": "{indicatorInfos['exampleUnit']}",
            "page_number": "92", //page number, where the respective information was found.
            "section" : "{indicatorInfos['exampleSourceSection']}" //text section where you found the information.
      }}

      General instructions:
      -Use the provided document as a source of metrics
      -Only consider english text
      -You are much better at reading tables and text than at interpreting figures. Knowing this, you prefer reading information from tables and text over using figures, if possible.
      -Hint: Look for tables in the Appendix and Annexes section of the reports, which can often be found in the last chapter of the documents. Look for tables in sections such as GRI indicatos, SASB Indicators, TCFD Indicators. These tables contain reliable and easy to digest information.
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