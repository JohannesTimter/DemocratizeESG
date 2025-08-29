from google import genai
from google.genai import types
from google.genai.types import GenerateContentConfig
from GroundTruth import loadSheet

#from loadGDriveSheet import loadGoogleSheet
from pydantic import BaseModel

import CompanyReportFile
#from storeGroundTruth import storeMetricExtrationRow
from Fullcontext_main import insertIntoMetricExtraction

class IndicatorExtraction(BaseModel):
  indicator_id: str
  value: str
  unit: str
  title_source_document: str
  page_number: str
  section: str

client = genai.Client(api_key="AIzaSyDBwyLmuojZQk0a1RcTyc8pJ_-37BrGfKY")

def promptDocuments(documents: list[CompanyReportFile]):

  prompts = generatePromptlist()

  for prompt in prompts:
    for doc in documents:
      if doc.topic == CompanyReportFile.Topic.ESG:
        print(f"Name: {doc.company_name}, Period: {doc.period}, Type: {doc.mimetype}, Topic: {doc.topic}")

        response = client.models.generate_content(
          model="gemini-2.5-flash",
          contents=[
            types.Part.from_bytes(
              data=doc.file_value,
              mime_type='application/pdf',
            ),
            prompt],
          config=GenerateContentConfig(
            thinking_config=types.ThinkingConfig(
              include_thoughts=True
            ),
            response_mime_type = "application/json",
            response_schema = IndicatorExtraction
          )
        )


        print(response.text)
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

        insertIntoMetricExtraction("Airlines", doc.company_name, doc.period, parsed_indicator, response_metadata, thoughts)

def generatePromptlist():
  prompts = []

  indicators = loadSheet("1QoOHmD0nxb52BIVpKyniVdYej1W5o1-sNot7DpaBl2w", "IndustryAgnostricIndicators!A1:I")
  #print(indicators.columns)
  for index, row in indicators.iterrows():
    prompts.append(promptTemplate(row))

  return prompts

def promptTemplate(indicatorInfos):
  prompt = f""""Extract the following Information from the provided document:
      -{indicatorInfos['IndicatorName']}

      Provide the title of the document and the page number, where the respective information was found. 
      Also provide the text section where you found the information. 
      The required output format is JSON.
      Example:
      {{
            "indicator_id": "{indicatorInfos['IndicatorID']}": 
            "value": "{indicatorInfos['exampleValue']}",
            "unit": "{indicatorInfos['exampleUnit']}",
            "title_source_document": "Delta 2020 ESG Report",
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