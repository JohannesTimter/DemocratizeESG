
from pydantic import BaseModel
from mysql.connector import IntegrityError
import pandas as pd
from scipy.misc import dataset_methods

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
    industry_agnostic_indicators_sheet = loadSheet("1QoOHmD0nxb52BIVpKyniVdYej1W5o1-sNot7DpaBl2w", "IndustryAgnostricIndicators!A1:T")
    industry_agnostic_units_to_convert = industry_agnostic_indicators_sheet[industry_agnostic_indicators_sheet['isUnitConversion'] == "TRUE"]
    industry_agnostic_units_to_convert = industry_agnostic_units_to_convert[['IndicatorID', 'isUnitConversion', 'targetUnit']]

    industry_specific_indicators_sheet = loadSheet("1QoOHmD0nxb52BIVpKyniVdYej1W5o1-sNot7DpaBl2w", "IndustrySpecificIndicators!A1:K")
    industry_specific_indicators_sheet = industry_specific_indicators_sheet[industry_specific_indicators_sheet['isUnitConversion'] == "TRUE"]
    industry_specific_indicators_sheet = industry_specific_indicators_sheet[['IndicatorID', 'isUnitConversion', 'targetUnit']]

    units_to_convert = pd.concat([industry_agnostic_units_to_convert, industry_specific_indicators_sheet])

    return units_to_convert

def select_all_dataset_rows():
    sql_query = "SELECT * FROM democratizeesg.big_dataset_consolidated;"
    mycursor.execute(sql_query)
    results = mycursor.fetchall()

    return results

def select_multiplication_factor(source_unit, target_unit, value):
    #print(f"source_unit: {source_unit}, target_unit: {target_unit}")
    sql_query = "SELECT multiplication_factor FROM democratizeesg.unit_conversion WHERE source_unit = %s AND target_unit = %s;"
    val = source_unit, target_unit
    mycursor.execute(sql_query, val)
    results = mycursor.fetchall()

    if len(results) == 0:
        return None

    multiplication_factor = results[0][0]
    if multiplication_factor == 666: #Special case of MT CO2e
        if float(value) < 5000:
            multiplication_factor = 1 #It's probably mega tons
        else:
            multiplication_factor = 0.000001 #It's probably metric tons


    return multiplication_factor

def insert_into_new_table(groundtruth_row):
    try:
        sql = ("INSERT INTO big_dataset_consolidated_unit_converted3 (id, industry, company_name, year, indicator_id, not_disclosed, value, "
               "unit, pagenumber, source_title, text_section, input_token_count, output_token_count, thought_summary) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        val = (groundtruth_row[0], groundtruth_row[1], groundtruth_row[2], groundtruth_row[3], groundtruth_row[4],
               groundtruth_row[5], groundtruth_row[6], groundtruth_row[7], groundtruth_row[8],
               groundtruth_row[9], groundtruth_row[10], groundtruth_row[11], groundtruth_row[12], groundtruth_row[13])
        mycursor.execute(sql, val)

        mydb.commit()
    except IntegrityError as e:
        pass
        #print(f"{groundtruth_row[2]}, {groundtruth_row[3]}, {groundtruth_row[4]} already exists in target table")

def insert_into_unit_conversion_table(source_unit, target_unit, multiplication_factor):
    sql = "INSERT INTO unit_conversion (source_unit, target_unit, multiplication_factor) VALUES (%s, %s, %s)"
    val = (source_unit, target_unit, multiplication_factor)
    try:
        mycursor.execute(sql, val)
        mydb.commit()
        print(f"Inserted new entry into unit_conversion table: {source_unit}, {target_unit}, {multiplication_factor}")
    except IntegrityError as e:
        print("IntegrityError")

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
            #print(thoughts)

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
                
                If it is not possible to generate a conversion factor from the source unit to the target unit, you return 0.0 as the multiplication_factor.
                If you are not sure what you are looking for, please stay on the safe side and just return 0.0. 
                For example, you can't convert tonnes to GWh, without knowing what substance we have measured the weight of.
                
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
    try:
        groundtruth_row[6] = str(round(float(old_value) * float(multiplication_factor), 2))
    except ValueError as e:
        print(e)
    groundtruth_row[7] = target_unit
    #print(f"{old_value} {source_unit} ---> {groundtruth_row[6]} {target_unit}")

def clean_number_string(dataset_row):
    text = str(dataset_row[6])
    #text = dataset_row
    text = fix_words(dataset_row, text)
    text = fix_dots_commas(text)

    #print(f"{input} -----> {text}")
    return text

def fix_dots_commas(text):
    count_dot = text.count('.')
    count_comma = text.count(',')

    # Case 1: Both separators are present (e.g., "3,636.0" or "65.574,681")
    # This is your original, most robust rule.
    if count_dot > 0 and count_comma > 0:
        if text.rfind('.') > text.rfind(','):
            # Dot is decimal, comma is thousands
            return text.replace(',', '')
        else:
            # Comma is decimal, dot is thousands
            return text.replace('.', '').replace(',', '.')

    # Case 2: Only commas are present (e.g., "10,000" or "13,478" or "123,45")
    elif count_comma > 0:
        if count_comma > 1:
            # Multiple commas, must be thousands separators
            return text.replace(',', '')
        else:
            # Single comma. Apply your new rule.
            parts = text.split(',')
            if len(parts[1]) == 3 and parts[0] != '0':
                # e.g., "13,478". Comma is thousands separator.
                return text.replace(',', '')
            else:
                # e.g., "123,45". Comma is decimal separator.
                return text.replace(',', '.')

    # Case 3: Only dots are present (e.g., "65.574.681" or "1.234" or "123.45")
    elif count_dot > 0:
        if count_dot > 1:
            # Multiple dots, must be thousands separators
            return text.replace('.', '')
        else:
            # Single dot. Apply your new rule.
            parts = text.split('.')
            if len(parts[1]) == 3 and parts[0] != '0':
                # e.g., "1.234". Dot is thousands separator.
                return text.replace('.', '')
            else:
                # e.g., "123.45". Dot is decimal separator.
                return text

    # Case 4: No separators (e.g., "12345")
    else:
        return text

def fix_words(dataset_row, text):
    # Remove unwanted symbols in the number
    unwanted_symbols = ['more than', "More than", "at least", ' ', '(', ')', '<', '>', 'around', 'over', 'under', '$',
                        '+', '\n', 'nearly']
    for unwanted_symbol in unwanted_symbols:
        text = text.replace(unwanted_symbol, '')
    # Move number words to unit
    number_words = ["million", "billion", "BILLION", "trillion", "triliun", "thousand", "m", "k", "M", "K", "bn", "B"]
    for number_word in number_words:
        if number_word in text:
            text = text.replace(number_word, "")
            dataset_row[7] = f"{number_word} {dataset_row[7]}"
    return text

def main():
    indicators_to_convert = find_indicators_to_convert()
    dataset_all_rows = select_all_dataset_rows()

    for dataset_row in dataset_all_rows:
        dataset_row_indicator_id = dataset_row[4]
        if (indicators_to_convert['IndicatorID'] == dataset_row_indicator_id).any(): #Indicator should be convertet
            not_disclosed = dataset_row[5]
            if not_disclosed == 0: #Indicator is disclosed
                dataset_row = list(dataset_row)
                dataset_row[6] = clean_number_string(dataset_row)
                source_unit = dataset_row[7]
                try:
                    target_unit_rows = indicators_to_convert[indicators_to_convert['IndicatorID'] == dataset_row_indicator_id]
                    target_unit = target_unit_rows.head(1)['targetUnit'].item()
                except ValueError as e:
                    print(e)
                if target_unit is not None and source_unit != target_unit:
                    multiplication_factor = select_multiplication_factor(source_unit, target_unit, dataset_row[6])

                    if multiplication_factor is None: #We should do a conversion based on the indicator, but don't have a factor yet
                        multiplication_factor = prompt_gemini_for_conversion_factor(source_unit, target_unit)
                        insert_into_unit_conversion_table(source_unit, target_unit, multiplication_factor)

                    if multiplication_factor != 0 and multiplication_factor is not None: #conversion is possible for this source unit and we should do conversion based on the indicator
                        update_unit_value(dataset_row, multiplication_factor, source_unit, target_unit)

        insert_into_new_table(dataset_row)

#def main2():
    #print(clean_number_string("17.526"))


if __name__ == "__main__":
    #main2()
    main()