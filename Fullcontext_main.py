import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="MyN3wP4ssw0rd",
  database="democratizeesg"
)

mycursor = mydb.cursor()

def insertIntoGroundtruth(basic_info, groundtruthreport_row):
    sql = ("INSERT INTO groundtruth2 (industry, company_name, year, indicator_id, not_disclosed, value, "
           "unit, searchword, pagenumber, source_title, source_link, notes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    val = (basic_info['Industry'], basic_info['Company'], basic_info['Year'], groundtruthreport_row['IndicatorID'], convertNotDisclosed(groundtruthreport_row['notDisclosed']),
            groundtruthreport_row['Value'], groundtruthreport_row['Unit'], groundtruthreport_row['Searchword'], groundtruthreport_row['Pagenumber'],
            groundtruthreport_row['SourceTitle'], groundtruthreport_row['LinkToSource'], '')
    mycursor.execute(sql, val)

    mydb.commit()

    #print(mycursor.rowcount, f"{basic_info['Company']} record inserted.")

def convertNotDisclosed(not_disclosed):
    if not_disclosed == "TRUE":
        return 1
    else:
        return 0

def insertIntoMetricExtraction(industry, companyName, year, parsed_indicator, response_metadata, thoughts):
    sql = ("INSERT INTO extraction_attempt2 (industry, company_name, year, indicator_id, not_disclosed, value, "
           "unit, pagenumber, source_title, text_section, cached_content_token_count, total_token_count, thought_summary)"
           " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    val = (industry, companyName, year, parsed_indicator.indicator_id, 1,
            parsed_indicator.value, parsed_indicator.unit, parsed_indicator.page_number,
            parsed_indicator.title_source_document, parsed_indicator.section, response_metadata.cached_content_token_count,
            response_metadata.total_token_count, thoughts)
    mycursor.execute(sql, val)

    mydb.commit()

    print(mycursor.rowcount, f"{companyName} record inserted.")