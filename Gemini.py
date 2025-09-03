import asyncio
import time

import httpx
from google import genai
from google.genai import types
from google.genai.types import GenerateContentConfig
from GroundTruth import loadSheet
from pydantic import BaseModel

import CompanyReportFile
from MySQL_client import insertIntoMetricExtraction, selectDisclosedIndicatorIDs

max_retries = 3
initial_delay_seconds = 2

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
    prompts = generatePromptlist(doc)
    tasks = []
    for indicatorID in prompts:
      task = getGeminiResponseAsync(doc, prompts, indicatorID)
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


async def getGeminiResponseAsync(doc, prompts, indicatorID):
  response = None
  start = 0
  for attempt in range(max_retries):
    start = time.time()
    try:
      response = await client.aio.models.generate_content(
        model="gemini-2.5-flash",
        contents=[
          types.Part.from_bytes(
            data=doc.file_value,
            mime_type=doc.mimetype,
          ),
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
      print(f"Caught a (Resource error?): {e.code}. Sleeping for a minute")
      if attempt < max_retries - 1:
        await asyncio.sleep(60)

  end = time.time()
  elapsed_time = int(end - start)
  print(response.text.replace('\n', ' ').replace('\r', ''))
  print(f"Elapsed time: {elapsed_time} s")
  return response, elapsed_time, indicatorID

def promptDocuments(documents: list[CompanyReportFile]):

  for doc in documents:
    prompts = generatePromptlist(doc)

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

def generatePromptlist(doc: CompanyReportFile):
  prompts = {}

  indicators = loadSheet("1QoOHmD0nxb52BIVpKyniVdYej1W5o1-sNot7DpaBl2w", "IndustryAgnostricIndicators!A1:I")
  alreadyDisclosedIndicators = selectDisclosedIndicatorIDs(doc)

  if doc.topic == CompanyReportFile.Topic.FINANCIAL:
    finance_indicators = ["lowCarbon_revenue", "environmental_ex", "revenue", "profit", "employees"]
    indicators = indicators.loc[indicators['IndicatorID'].isin(finance_indicators)]

  for index, row in indicators.iterrows():
    if row['IndicatorID'] not in alreadyDisclosedIndicators:
      prompts[row['IndicatorID']] = promptTemplate(row)

  print(f"Following indicators are already disclosed and will not be prompted again: {",".join(alreadyDisclosedIndicators)}")

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


if __name__ == "__main__":
  prompts = generatePromptlist()
  for prompt in prompts:
    print(prompt)
    print("----")