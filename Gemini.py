from google import genai
from google.genai import types
from google.genai.types import GenerateContentConfig
from GroundTruth import loadSheet
from pydantic import BaseModel

import CompanyReportFile
from MySQL_client import insertIntoMetricExtraction, selectDisclosedIndicatorIDs


class IndicatorExtraction(BaseModel):
  isDisclosed: int = 1
  indicator_id: str
  value: str
  unit: str
  page_number: str
  section: str

client = genai.Client(api_key="AIzaSyDBwyLmuojZQk0a1RcTyc8pJ_-37BrGfKY")

def promptDocuments(documents: list[CompanyReportFile]):

  for doc in documents:
    prompts = generatePromptlist(doc)

    for indicatorID in prompts:
      print(f"Name: {doc.company_name}, Period: {doc.period}, Type: {doc.mimetype}, Topic: {doc.topic}")

      response = client.models.generate_content(
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
      parsed_indicator.indicator_id = indicatorID

      insertIntoMetricExtraction(doc, parsed_indicator, response_metadata, thoughts)

def generatePromptlist(doc: CompanyReportFile):
  prompts = {}

  indicators = loadSheet("1QoOHmD0nxb52BIVpKyniVdYej1W5o1-sNot7DpaBl2w", "IndustryAgnostricIndicators!A1:I")
  alreadyDisclosedIndicators = selectDisclosedIndicatorIDs(doc)

  #print(indicators.columns)
  for index, row in indicators.iterrows():
    if row['IndicatorID'] not in alreadyDisclosedIndicators:
      prompts[row['IndicatorID']] = promptTemplate(row)
    else:
      print(f"{row['IndicatorID']} is already disclosed, not prompting it again.")

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