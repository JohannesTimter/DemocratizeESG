from google.genai import types
from google.genai.types import GenerateContentConfig

from GroundTruth import loadSheet
from MySQL_client import mycursor
from google import genai
client = genai.Client()

def main():
    print("Hello")
    potentialConflicts = fetchPotentialConflicts()
    potentialConflictDetails = fetchConflictDetails(potentialConflicts)

    #print(potentialConflictDetails)

    realConflicts, resolvedSimpleConflicts = resolveSimpleConflicts(potentialConflictDetails) #if value and unit are the same already!
    resolvedConflicts = resolveConflicts(realConflicts) #Disagreement for value or unit -> use Gemini for Resolution

    insertResults(resolvedConflicts, resolvedSimpleConflicts)

def fetchPotentialConflicts():
    sql_query = ("SELECT company_name, year, indicator_id, not_disclosed, GROUP_CONCAT(value SEPARATOR ' vs. '), group_concat(unit SEPARATOR ' vs. ') "
                 "FROM democratizeesg.extraction_attempt3_unconsolidated "
                 "GROUP BY company_name, year, indicator_id, not_disclosed "
                 "HAVING COUNT(*) > 1 AND not_disclosed = 0")
    mycursor.execute(sql_query)
    results = mycursor.fetchall()

    return results

def fetchConflictDetails(potentialConflicts):
    potentialConflictDetails = []

    for potentialConflict in potentialConflicts:
        sql_query = (
            "SELECT * FROM democratizeesg.extraction_attempt3_unconsolidated "
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
            values.append(potentialConflictCandidate[5])
            units.append(potentialConflictCandidate[6])
        if all_same(values) and all_same(units):
            resolvedSimpleConflicts.append(potentialConflictGroup[0]) #Just use the first record from now on, discard the second one
        else:
            realConflicts.append(potentialConflictGroup)

    return realConflicts, resolvedSimpleConflicts

def resolveConflicts(realConflicts):
    resolvedConflicts = []

    indicatorInfos = loadSheet("1QoOHmD0nxb52BIVpKyniVdYej1W5o1-sNot7DpaBl2w", "IndustryAgnostricIndicators!A1:J")

    for conflictGroup in realConflicts:
        prompt = generateConflictResolutionPrompt(conflictGroup, indicatorInfos)
        response = client.models.generate_content(
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
        print(response)

def generateConflictResolutionPrompt(conflictGroup, indicatorInfos):
    optionsString = ""
    for index, conflictCandidate in enumerate(conflictGroup):
        optionsString += f"Option {index}:\n"
        optionsString += f"value: {conflictCandidate[5]}\n"
        optionsString += f"unit: {conflictCandidate[6]}\n"
        optionsString += f"text section used: {conflictCandidate[6]}\n"
        optionsString += f"thoughts: {conflictCandidate[12]}\n"
        optionsString += f"-------------------\n"

    for index, indicatorRow in indicatorInfos.iterrows():
        if indicatorRow['IndicatorID'] == conflictGroup[0][3]:
            break

    promptTemplate = \
        f""""You are an ESG expert and your task is to decide, which one of the following, 
            conflicting solutions contains the most correct value and unit. Answer by naming the index of the most correct solution, this will be either 0, 1 or 2. 
            For example, if you think Option 2 is the best solution, simply answer: "2".
            You have to chose exactly one solution. You can use the text section used and the thoughts to estimate, 
            how confident the authors were in their answer.

            The solutions want to measure the following metric of a reporting company: {indicatorRow['IndicatorName']}
            Some general information about the indicator: {indicatorRow['IndicatorDescription']}\n

            {optionsString}
            """
    return promptTemplate


def insertResults(resolvedConflicts, resolvedSimpleConflicts):
    pass

def all_same(items):
  return len(set(items)) <= 1

if __name__ == '__main__':
    main()