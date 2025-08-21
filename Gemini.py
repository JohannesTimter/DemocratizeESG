from google import genai
from google.genai import types


import CompanyReportFile

client = genai.Client()

def summarizeDoc(documents: list[CompanyReportFile]):

  prompt = "Briefly summarize each of the attached documents"

  contents = []
  for doc in documents:
    print(f"Name: {doc.company_name}, Period: {doc.period}")
    contents.append(
      types.Part.from_bytes(
        data=doc.file_value,
        mime_type=doc.mimetype,
      ))
  contents.append(prompt)


  response = client.models.generate_content(
    model="gemini-2.5-flash",
    contents=contents)
  print(response.text)