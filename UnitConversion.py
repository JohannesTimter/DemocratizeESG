
from pydantic import BaseModel
from mysql.connector import IntegrityError

from GroundTruth import loadSheet
import mysql.connector
from google import genai
from google.genai import types
from google.genai.types import GenerateContentConfig
indicators_sheet_id = "1QoOHmD0nxb52BIVpKyniVdYej1W5o1-sNot7DpaBl2w" #Sheet with list of groundtruth sheets
indicators_sheet_range = "IndustryAgnostricIndicators!A1:J"

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="MyN3wP4ssw0rd",
  database="democratizeesg"
)

mycursor = mydb.cursor()
client = genai.Client()

class ConversionFactor(BaseModel):
    multiplication_factor: float

def find_indicators_to_convert():
    indicators_sheet = loadSheet("1QoOHmD0nxb52BIVpKyniVdYej1W5o1-sNot7DpaBl2w", "IndustryAgnostricIndicators!A1:T")
    units_to_convert = indicators_sheet[indicators_sheet['isUnitConversion'] == "TRUE"]
    units_to_convert = units_to_convert[['IndicatorID', 'isUnitConversion', 'targetUnit']]

    return units_to_convert

def select_all_groundtruth_rows():
    sql_query = "SELECT * FROM democratizeesg.groundtruth4;"
    mycursor.execute(sql_query)
    results = mycursor.fetchall()

    return results

def select_multiplication_factor(source_unit, target_unit):
    #print(f"source_unit: {source_unit}, target_unit: {target_unit}")
    sql_query = "SELECT multiplication_factor FROM democratizeesg.unit_conversion WHERE source_unit = %s AND target_unit = %s;"
    val = source_unit, target_unit
    mycursor.execute(sql_query, val)
    results = mycursor.fetchall()

    if len(results) == 0:
        return None

    return results[0][0]

def insert_into_new_table(groundtruth_row):
    try:
        sql = ("INSERT INTO groundtruth4_unit_converted (id, industry, company_name, year, indicator_id, not_disclosed, value, "
               "unit, searchword, pagenumber, source_title, source_link, notes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        val = (groundtruth_row[0], groundtruth_row[1], groundtruth_row[2], groundtruth_row[3], groundtruth_row[4],
               groundtruth_row[5], groundtruth_row[6], groundtruth_row[7], groundtruth_row[8],
               groundtruth_row[9], groundtruth_row[10], groundtruth_row[11], '')
        mycursor.execute(sql, val)

        mydb.commit()
    except IntegrityError as e:
        print(f"{groundtruth_row[2]}, {groundtruth_row[3]}, {groundtruth_row[4]} already exists in target table")

def insert_into_unit_conversion_table(source_unit, target_unit, multiplication_factor):
    sql = "INSERT INTO unit_conversion (source_unit, target_unit, multiplication_factor) VALUES (%s, %s, %s)"
    val = (source_unit, target_unit, multiplication_factor)
    mycursor.execute(sql, val)

    mydb.commit()
    print(f"Inserted new entry into unit_conversion table: {source_unit}, {target_unit}, {multiplication_factor}")

def send_request(prompt):
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=[prompt],
        config=GenerateContentConfig(
            thinking_config=types.ThinkingConfig(
                include_thoughts=True
            ),
            response_mime_type="application/json",
            response_schema=ConversionFactor
        )
    )
    for part in response.candidates[0].content.parts:
        if not part.text:
            continue
        if part.thought:
            thoughts = part.text
            print(thoughts)

    parsed_response: ConversionFactor = response.parsed
    return parsed_response

def select_relevant_examples(target_unit):
    sql_query = "SELECT * FROM democratizeesg.unit_conversion WHERE target_unit = %s;"
    val = target_unit,
    mycursor.execute(sql_query, val)
    results = mycursor.fetchall()

    formatted_results = ""
    for row in results:
        formatted_results = formatted_results +  f"source_unit: {row[0]}, target_unit: {row[1]}, multiplication_factor: {row[2]}"
        formatted_results = formatted_results +  f"\n-------------\n"

    return results

def create_unit_conversion_prompt(source_unit, target_unit):
    relevant_examples = select_relevant_examples(target_unit)

    prompt = f"""Find the multiplication factor to convert a value from the unit {source_unit} to {target_unit}.
                
                #Use "." as the decimal separator. Example for output format:
                {{            
                    "multiplication_factor": 3.55
                }}
                
                If there is really no way to get from the source unit to the target unit, you can return 0.0 as the multiplication_factor.
                
                #Here are a few examples from the existing conversion database:
                {relevant_examples}   
            """
    return prompt

def prompt_gemini_for_conversion_factor(source_unit, target_unit):
    prompt = create_unit_conversion_prompt(source_unit, target_unit)
    response: ConversionFactor = send_request(prompt)

    return response.multiplication_factor

def update_unit_value(groundtruth_row, multiplication_factor, source_unit, target_unit):
    old_value = groundtruth_row[6]
    groundtruth_row[6] = str(round(float(old_value) * float(multiplication_factor), 2))
    groundtruth_row[7] = target_unit
    print(f"{old_value} {source_unit} ---> {groundtruth_row[6]} {target_unit}")

def clean_number_string(text):
    input = text
    text = str(text)

    # Remove any spaces in the number
    text.replace(' ', '')

    # Step 1: Handle mixed format (e.g., 1.187.923,68)
    if '.' in text and ',' in text:
        text = text.replace('.', '')

    # Step 2: Handle comma as thousands separator (e.g., 13,532,370 or 1,234)
    # Check for multiple commas or a single comma followed by 3 digits
    if text.count(',') > 1 or (text.count(',') == 1 and len(text.split(',')[1]) == 3):
        text = text.replace(',', '')

    # Step 3: Standardize decimal comma to a dot (e.g., 33,3 or the result from step 1)
    text = text.replace(',', '.')

    # If there are multiple points and no commas (e.g. 65.574.681) remove all of them
    if text.count('.') > 1 and text.count(',') == 0:
        text = text.replace('.', '')

    #print(f"{input} -----> {text}")
    return text


def main():
    indicators_to_convert = find_indicators_to_convert()
    groundtruth_all_rows = select_all_groundtruth_rows()

    for groundtruth_row in groundtruth_all_rows:
        groundtruth_row_indicator_id = groundtruth_row[4]
        if (indicators_to_convert['IndicatorID'] == groundtruth_row_indicator_id).any(): #Indicator should be convertet
            not_disclosed = groundtruth_row[5]
            if not_disclosed == 0: #Indicator is disclosed
                groundtruth_row = list(groundtruth_row)
                groundtruth_row[6] = clean_number_string(groundtruth_row[6])
                source_unit = groundtruth_row[7]
                target_unit = indicators_to_convert[indicators_to_convert['IndicatorID'] == groundtruth_row_indicator_id]['targetUnit'].item()
                if source_unit != target_unit:
                    multiplication_factor = select_multiplication_factor(source_unit, target_unit)

                    if target_unit is not None and multiplication_factor is None: #We should do a conversion based on the indicator, but don't have a factor
                        multiplication_factor = prompt_gemini_for_conversion_factor(source_unit, target_unit)
                        insert_into_unit_conversion_table(source_unit, target_unit, multiplication_factor)
                        update_unit_value(groundtruth_row, multiplication_factor, source_unit, target_unit)
                    elif multiplication_factor != 0 and multiplication_factor is not None: #conversion is possible for this source unit and we should do conversion based on the indicator
                        update_unit_value(groundtruth_row, multiplication_factor, source_unit, target_unit)

        insert_into_new_table(groundtruth_row)


if __name__ == "__main__":
    main()