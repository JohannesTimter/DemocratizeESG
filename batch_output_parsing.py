from google import genai
from Gemini import IndicatorExtraction
from MySQL_client import insertIntoBatchMetricExtraction
import json
client = genai.Client()

def parse_response(response_json):
    responseData_dic = {}

    response_json['key'] = response_json['key'].replace('Rolls-Royce', 'Rolls Royce')

    doc_properties = response_json['key'].split('-')
    responseData_dic['company_name'] = doc_properties[0]
    responseData_dic['year'] = doc_properties[1]
    responseData_dic['indicator_id'] = doc_properties[4]
    responseData_dic['source_title'] = doc_properties[0] + "_" + doc_properties[1] + "_" + doc_properties[2] + "_" + doc_properties[3]

    #print(responseData_dic['source_title'])


    if 'error' in response_json:
        print(response_json['error'])
        responseData_dic['isDisclosed'] = 0
        responseData_dic['value'] = ""
        responseData_dic['unit'] = ""
        responseData_dic['page_number'] = ""
        responseData_dic['section'] = ""
        responseData_dic['thoughts'] = "Error: " + response_json['error']['message']
        responseData_dic['inputTokenCount'] = 0
        responseData_dic['outputTokenCount'] = 0
    elif response_json['response']['candidates'][0]['finishReason'] == 'RECITATION' or response_json['response']['candidates'][0]['finishReason'] == 'MAX_TOKENS':
        responseData_dic['isDisclosed'] = 0
        responseData_dic['value'] = ""
        responseData_dic['unit'] = ""
        responseData_dic['page_number'] = ""
        responseData_dic['section'] = ""
        responseData_dic['thoughts'] = "Error: " + response_json['response']['candidates'][0]['finishReason']
        responseData_dic['inputTokenCount'] = 0
        responseData_dic['outputTokenCount'] = 0
    else:
        response_text = ""
        response_parts = response_json['response']['candidates'][0]['content']['parts']
        for response_part in response_parts:
            if 'thought' in response_part:
                responseData_dic['thoughts'] = response_part['text']
            else:
                response_text = response_part['text']

        response: IndicatorExtraction = json.loads(response_text)
        if "isDisclosed" not in response:
            #print(f"isDisclosed not specified! Setting it to 1")
            if "not disclosed" in response['value'].lower() or "n/a" in response['value'].lower():
                response["isDisclosed"] = 0
            else:
                response["isDisclosed"] = 1


        responseData_dic['isDisclosed'] = response['isDisclosed']
        responseData_dic['value'] = response['value']
        responseData_dic['unit'] = response['unit']
        responseData_dic['page_number'] = response['page_number']
        responseData_dic['section'] = response['section']

        if "thoughts" not in responseData_dic:
            print(f"WTF! Keine thoughts in gefunden!")
            responseData_dic["thoughts"] = f"Error: No thoughts found"
        if "promptTokenCount" in response_json['response']["usageMetadata"]:
            responseData_dic['inputTokenCount'] = response_json['response']["usageMetadata"]["promptTokenCount"]
        else:
            responseData_dic['inputTokenCount'] = 0
        responseData_dic['outputTokenCount'] = response_json['response']["usageMetadata"]["candidatesTokenCount"] + response_json['response']["usageMetadata"]["thoughtsTokenCount"]

    return responseData_dic

def main():
    # The output is in another file.
    result_file_name = "files/batch-y0exp4p7vjvlue8c6dzfekb04ne6990xjwf7"
    print(f"Results are in file: {result_file_name}")

    print("\nDownloading and parsing result file content...")
    file_content_bytes = client.files.download(file=result_file_name)
    file_content = file_content_bytes.decode('utf-8')

    parsed_response = ""

    # The result file is also a JSONL file. Parse and print each line.
    for line in file_content.splitlines():
        if line:
            try:
                responseData_dic = parse_response(json.loads(line))
                insertIntoBatchMetricExtraction(responseData_dic)
            except json.JSONDecodeError as e:
                print(e)

if __name__ == "__main__":
    main()