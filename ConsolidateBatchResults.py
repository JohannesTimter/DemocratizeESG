import asyncio
import json
import time

import httpx
from google.genai import types
from google.genai.types import GenerateContentConfig
from mysql.connector import IntegrityError
from pydantic import BaseModel
import pandas as pd
from GroundTruth import loadSheet
from MySQL_client import mycursor, mydb
from google import genai
client = genai.Client()

class ConflictResolution(BaseModel):
  OptionIndex: int


async def main():
    print("Starting Consolidation Process")

    potentialConflicts = fetchPotentialConflicts()
    potentialConflictDetails = fetchConflictDetails(potentialConflicts)

    realConflicts, resolvedSimpleConflicts = resolveSimpleConflicts(potentialConflictDetails) #if value and unit are the same already!

    #createConflictResolutionJSON(realConflicts)
    resolvedConflicts = resolveConflictsBatch(realConflicts)


    #resolvedConflicts = await resolveConflictsAsync(realConflicts)

    nonConflictRecords = fetchNonConflictRecords() #Metrics that are disclosed in only one source
    transferRecords(resolvedConflicts, resolvedSimpleConflicts, nonConflictRecords)
    insertUndisclosedRecords()

    print("Consolidation Process Finished")

def loadChosenIndices():
    result_file_names = ["files/batch-y0exp4p7vjvlue8c6dzfekb04ne6990xjwf7", "files/batch-fuito7nyzmnxn9lpi1r0ekzztnswm1oxu9u7",
                         "files/batch-kzgxobx825i2hr62w3wgp5mnll1w0mp0kmm4", "files/batch-tr3z04w688ok4jpouc540a8lluvatjdezs0f"]

    all_files_content = ""

    for result_file_name in result_file_names:
        print("Downloading " + result_file_name)
        file_content_bytes = client.files.download(file=result_file_name)
        file_content = file_content_bytes.decode('utf-8')
        all_files_content += file_content

    "\n".join(all_files_content)

    chosen_indices = {}

    for line in all_files_content.splitlines():
        if line:
            try:
                response_json = json.loads(line)
                response_text = ""
                response_parts = response_json['response']['candidates'][0]['content']['parts']
                for response_part in response_parts:
                    if 'thought' in response_part:
                        continue
                    else:
                        response_text = response_part['text']
                picked_index_json = json.loads(response_text)
                chosen_indices[response_json["key"]] = (int(picked_index_json['OptionIndex']))
            except json.JSONDecodeError as e:
                print(e)


    return chosen_indices

def resolveConflictsBatch(realConflicts):
    chosen_indices = loadChosenIndices()
    resolvedConflicts = []

    for conflictGroup in realConflicts:
        company = conflictGroup[0][2]
        year = conflictGroup[0][3]
        indicator_id = conflictGroup[0][4]
        resolutionIndex = chosen_indices[f"{company}-{year}-{indicator_id}"]
        resolvedConflicts.append(conflictGroup[resolutionIndex][0])

    return resolvedConflicts

def createConflictResolutionJSON(realConflicts):
    requests_data = []
    industry_agnostic_indicators = loadSheet("1QoOHmD0nxb52BIVpKyniVdYej1W5o1-sNot7DpaBl2w", "IndustryAgnostricIndicators!B1:E")
    industry_specific_indicators = loadSheet("1QoOHmD0nxb52BIVpKyniVdYej1W5o1-sNot7DpaBl2w","IndustrySpecificIndicators!B1:E")
    #all_indicators = industry_agnostic_indicators + industry_specific_indicators
    all_indicators = pd.concat([industry_agnostic_indicators, industry_specific_indicators])

    for conflictGroup in realConflicts:
        company = conflictGroup[0][2]
        year = conflictGroup[0][3]
        indicator_id = conflictGroup[0][4]
        prompt = generateConflictResolutionPrompt(conflictGroup, all_indicators)
        request = {
            "key": f"{company}-{year}-{indicator_id}",
            "request": {
                "contents": [{
                    "parts": [
                        {"text": prompt}
                    ]
                }],
                "generationConfig": {
                    "thinking_config": {
                        "include_thoughts": True,
                        "thinking_budget": -1
                    },
                    "response_mime_type": "application/json",
                    "response_json_schema": {
                            "type": "object",
                            "properties": {
                              "OptionIndex": {
                                "type": "integer",
                                "description": "The zero-based index for a selected option."
                              }
                            }
                          }
                }
            }
        }
        requests_data.append(request)

    json_file_path = 'batch_input_output_files/big_dataset_conflict_resolution.json'
    print(f"Writing JSONL file: {json_file_path}")
    with open(json_file_path, 'w') as f:
        for req in requests_data:
            f.write(json.dumps(req) + '\n')

async def resolveConflictsAsync(realConflicts):
    tasks = []
    index = 0
    industry_agnostic_indicators = loadSheet("1QoOHmD0nxb52BIVpKyniVdYej1W5o1-sNot7DpaBl2w", "IndustryAgnostricIndicators!A1:J")
    industry_specific_indicators = loadSheet("1QoOHmD0nxb52BIVpKyniVdYej1W5o1-sNot7DpaBl2w","IndustrySpecificIndicators!A1:J")

    for conflictGroup in realConflicts:
        industry_specific_indicators = industry_specific_indicators.loc[industry_specific_indicators['Industry'] == conflictGroup[0][1]]
        all_indicators = industry_agnostic_indicators + industry_specific_indicators
        index += 1
        task = resolveConflict(conflictGroup, all_indicators, index)
        tasks.append(task)

    resolvedConflicts = await asyncio.gather(*tasks)  # Disagreement for value or unit -> use Gemini for Resolution

    return resolvedConflicts

def fetchPotentialConflicts():
    sql_query = ("SELECT company_name, year, indicator_id, not_disclosed, GROUP_CONCAT(value SEPARATOR ' vs. '), group_concat(unit SEPARATOR ' vs. ') "
                 "FROM big_dataset_unconsolidated "
                 "GROUP BY company_name, year, indicator_id, not_disclosed "
                 "HAVING not_disclosed = 0 AND COUNT(*) > 1")
    mycursor.execute(sql_query)
    results = mycursor.fetchall()

    print(f"Fount {len(results)} potential conflicts")

    return results

def fetchConflictDetails(potentialConflicts):
    potentialConflictDetails = []

    for potentialConflict in potentialConflicts:
        sql_query = (
            "SELECT * FROM big_dataset_unconsolidated "
            "WHERE company_name = %s "
            "AND year = %s "
            "AND indicator_id = %s")
        val = potentialConflict[0], potentialConflict[1], potentialConflict[2]
        mycursor.execute(sql_query, val)
        results = mycursor.fetchall()
        potentialConflictDetails.append(results)

    return potentialConflictDetails

def resolveSimpleConflicts(potentialConflictDetails):
    resolvedSimpleConflicts = []
    realConflicts = []

    for potentialConflictGroup in potentialConflictDetails:
        values = []
        units = []
        for potentialConflictCandidate in potentialConflictGroup:
            values.append(potentialConflictCandidate[6])
            units.append(potentialConflictCandidate[7])
        if all_same(values) and all_same(units):
            resolvedSimpleConflicts.append(potentialConflictGroup[0][0]) #Just use the first record from now on, discard other ones
        else:
            realConflicts.append(potentialConflictGroup)

    print(f"Resolved {len(resolvedSimpleConflicts)} simple conflicts")
    print(f"{len(realConflicts)} real conflicts remain")

    return realConflicts, resolvedSimpleConflicts

async def resolveConflict(conflictGroup, indicatorInfos, conflictIndex):
    prompt = generateConflictResolutionPrompt(conflictGroup, indicatorInfos)
    response = await client.aio.models.generate_content(
        model="gemini-2.5-flash",
        contents=[prompt],
        config=GenerateContentConfig(
            thinking_config=types.ThinkingConfig(
                include_thoughts=True
            ),
        response_mime_type="application/json",
        response_schema={
                            "type": "object",
                            "properties": {
                              "OptionIndex": {
                                "type": "integer",
                                "description": "The zero-based index for a selected option."
                              }
                            }
                          }
        )
    )
    print(response.text)

    thoughts = ""
    for part in response.candidates[0].content.parts:
        if not part.text:
            continue
        if part.thought:
            thoughts = part.text

    parsed_response: ConflictResolution = response.parsed
    resolutionIndex = parsed_response['OptionIndex']

    print(f"Resolved conflict no. {conflictIndex}")
    return conflictGroup[resolutionIndex][0]

async def resolveConflictBatchJSON(conflictGroup, indicatorInfos, conflictIndex):
    prompt = generateConflictResolutionPrompt(conflictGroup, indicatorInfos)
    response = await client.aio.models.generate_content(
        model="gemini-2.5-flash",
        contents=[prompt],
        config=GenerateContentConfig(
            thinking_config=types.ThinkingConfig(
                include_thoughts=True
            ),
        response_mime_type="application/json",
        response_schema={
                            "type": "object",
                            "properties": {
                              "OptionIndex": {
                                "type": "integer",
                                "description": "The zero-based index for a selected option."
                              }
                            }
                          }
        )
    )
    print(response.text)

    thoughts = ""
    for part in response.candidates[0].content.parts:
        if not part.text:
            continue
        if part.thought:
            thoughts = part.text

    parsed_response: ConflictResolution = response.parsed
    resolutionIndex = parsed_response['OptionIndex']

    print(f"Resolved conflict no. {conflictIndex}")
    return conflictGroup[resolutionIndex][0]

def generateConflictResolutionPrompt(conflictGroup, indicatorInfos):
    optionsString = ""
    for index, conflictCandidate in enumerate(conflictGroup):
        optionsString += f"Option {index}:\n"
        optionsString += f"value: {conflictCandidate[6]}\n"
        optionsString += f"unit: {conflictCandidate[7]}\n"
        optionsString += f"text section used: {conflictCandidate[10]}\n"
        optionsString += f"thoughts: {conflictCandidate[13]}\n"
        optionsString += f"-------------------\n"

    for index, indicatorRow in indicatorInfos.iterrows():
        if indicatorRow['IndicatorID'] == conflictGroup[0][4]:
            break

    promptTemplate = \
        f""""You are an ESG expert and your task is to decide, which one of the following, 
            conflicting solutions contains the most correct value and unit.
            Answer by naming the index of the most correct solution, this will be either 0, 1, 2, ... and so on. 
            For example, if you think Option 2 is the best solution, simply answer: "2".
            You must choose exactly one solution. You can use the text section used and the thoughts to estimate, 
            how confident the authors were in their answer. Generally, values that were explicitly stated in 
            the source document are preferable over inferred/calculated values. Each option was generated using a different report as a source.
            So if one option has a valid solution, and another option claims that the metric is not disclosed, you should prefer the valid solution.
                        
            
            The solutions should measure the following metric of a reporting company: {indicatorRow['IndicatorName']}
            Some general information about the indicator: {indicatorRow['IndicatorDescription']}\n
            {indicatorRow['PromptEngineering']}\n

            {optionsString}
            """
    return promptTemplate

def fetchNonConflictRecords():
    sql_query = ("SELECT t1.id "
                "FROM big_dataset_unconsolidated as t1 "
                "INNER JOIN "
                    "(SELECT company_name, year, indicator_id, not_disclosed "
                    " FROM big_dataset_unconsolidated "
                    " GROUP BY company_name, year, indicator_id, not_disclosed "
                    " HAVING  not_disclosed = 0 AND COUNT(*) = 1 "
                    ") AS t2 "
                "ON t1.company_name = t2.company_name "
                "AND t1.year = t2.year "
                "AND t1.indicator_id = t2.indicator_id " 
                "WHERE t1.not_disclosed = 0 ")

    mycursor.execute(sql_query)
    results = mycursor.fetchall()

    results = [row[0] for row in results]

    return results

def transferRecords(resolvedConflicts, resolvedSimpleConflicts, nonConflictRecords):
    disclosedRecordIDs = []
    disclosedRecordIDs.extend(resolvedConflicts)
    disclosedRecordIDs.extend(resolvedSimpleConflicts)
    disclosedRecordIDs.extend(nonConflictRecords)

    for disclosedRecordID in disclosedRecordIDs:
        sql = (
            "INSERT INTO big_dataset_consolidated (id, industry, company_name, year, indicator_id, not_disclosed, value, "
            "unit, pagenumber, source_title, text_section, input_token_count, output_token_count, thought_summary)"
            "SELECT id, industry, company_name, year, indicator_id, not_disclosed, value, unit, pagenumber, source_title, text_section, input_token_count, output_token_count, thought_summary "
            "FROM big_dataset_unconsolidated "
            "WHERE id = %s")
        val = [disclosedRecordID]
        mycursor.execute(sql, val)

        mydb.commit()

def insertUndisclosedRecords():
    undisclosedDuplicatesCounter = 0

    sql_query = ("SELECT * "
                "FROM big_dataset_unconsolidated "
                "WHERE not_disclosed = 1")

    mycursor.execute(sql_query)
    undisclosedRecords = mycursor.fetchall()

    for undisclosedRecord in undisclosedRecords:
        sql = (
            "INSERT INTO big_dataset_consolidated (id, industry, company_name, year, indicator_id, not_disclosed, value, "
            "unit, pagenumber, source_title, text_section, input_token_count, output_token_count, thought_summary)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        val = (undisclosedRecord[0], undisclosedRecord[1], undisclosedRecord[2],
               undisclosedRecord[3], undisclosedRecord[4], undisclosedRecord[5], None, None, undisclosedRecord[8],
               undisclosedRecord[9], undisclosedRecord[10], undisclosedRecord[11], undisclosedRecord[12], undisclosedRecord[13])
        try:
            mycursor.execute(sql, val)
            mydb.commit()
        except IntegrityError as err:
            undisclosedDuplicatesCounter +=1

    print(f"{undisclosedDuplicatesCounter} undisclosed records where duplicate and were not inserted.")

def all_same(items):
  return len(set(items)) <= 1

if __name__ == "__main__":
    asyncio.run(main())