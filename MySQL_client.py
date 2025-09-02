import mysql.connector
from mysql.connector import IntegrityError

from CompanyReportFile import CompanyReportFile

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

def convertIsDisclosed(is_disclosed):
    if is_disclosed == 0:
        return 1
    else:
        return 0

def createDocumentName(doc: CompanyReportFile) -> str:
    return doc.company_name + "_" + doc.topic + "_" + str(doc.period)

def selectDisclosedIndicatorIDs(doc: CompanyReportFile):
    sql_query = "SELECT indicator_id FROM extraction_attempt2 WHERE company_name = %s AND year = %s AND not_disclosed = 0"
    val = doc.company_name, doc.period,
    mycursor.execute(sql_query, val)
    results = mycursor.fetchall()

    indicator_ids = []
    for row in results:
        indicator_ids.append(row[0])

    return indicator_ids


def insertIntoMetricExtraction(sourceDoc: CompanyReportFile, parsed_indicator, response_metadata, thoughts):
    try:
        sql = ("INSERT INTO extraction_attempt2 (industry, company_name, year, indicator_id, not_disclosed, value, "
               "unit, pagenumber, source_title, text_section, cached_content_token_count, total_token_count, thought_summary)"
               " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        val = (sourceDoc.industry, sourceDoc.company_name, sourceDoc.period, parsed_indicator.indicator_id, convertIsDisclosed(parsed_indicator.isDisclosed),
                parsed_indicator.value, parsed_indicator.unit, parsed_indicator.page_number,
                createDocumentName(sourceDoc), parsed_indicator.section, response_metadata.cached_content_token_count,
                response_metadata.total_token_count, thoughts)
        mycursor.execute(sql, val)

        mydb.commit()
    except IntegrityError as e:
        if parsed_indicator.isDisclosed == 1:
            print(f"Updating {parsed_indicator.indicator_id} ")

            sql = ("UPDATE extraction_attempt2 "
                   "SET not_disclosed = %s, value = %s, unit = %s, pagenumber = %s, source_title = %s, text_section = %s, cached_content_token_count = %s, total_token_count = %s, thought_summary = %s "
                   "WHERE industry = %s AND company_name = %s AND year = %s AND indicator_id = %s")
            val = (convertIsDisclosed(parsed_indicator.isDisclosed), parsed_indicator.value, parsed_indicator.unit,
                   parsed_indicator.page_number, createDocumentName(sourceDoc), parsed_indicator.section,
                   response_metadata.cached_content_token_count, response_metadata.total_token_count, thoughts,
                   sourceDoc.industry, sourceDoc.company_name, sourceDoc.period, parsed_indicator.indicator_id)
            mycursor.execute(sql, val)

            mydb.commit()


    #print(mycursor.rowcount, f"{createDocumentName(sourceDoc)} record inserted.")