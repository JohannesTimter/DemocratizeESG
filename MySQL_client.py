import mysql.connector
from _mysql_connector import MySQLInterfaceError
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
    sql = ("INSERT INTO groundtruth3 (industry, company_name, year, indicator_id, not_disclosed, value, "
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
    name = "_".join([doc.company_name, doc.topic, doc.period])
    if doc.counter != 1:
        name += "_" + str(doc.counter)
    return name

def selectDisclosedIndicatorIDs(doc: CompanyReportFile):
    sql_query = "SELECT indicator_id FROM extraction_attempt2_test WHERE company_name = %s AND year = %s AND not_disclosed = 0"
    val = doc.company_name, doc.period,
    mycursor.execute(sql_query, val)
    results = mycursor.fetchall()

    indicator_ids = []
    for row in results:
        indicator_ids.append(row[0])

    return indicator_ids

def selectAvgInputTokenCount(source_title: str):
    sql_query = ("SELECT truncate(avg(input_token_count),0) "
                 "FROM democratizeesg.extraction_attempt3_unconsolidated "
                 "WHERE source_title = %s")
    val = source_title
    mycursor.execute(sql_query, val)
    results = mycursor.fetchall()

    avg_input_token_count = results[0][0]

    return avg_input_token_count

def insertIntoMetricExtraction(sourceDoc: CompanyReportFile, parsed_indicator, response_metadata, thoughts, elapsed_time):
    try:
        sql = ("INSERT INTO extraction_attempt3_test_unconsolidated (company_name, year, indicator_id, not_disclosed, value, "
               "unit, pagenumber, source_title, text_section, cached_content_token_count, total_token_count, thought_summary, elapsed_time)"
               " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        val = (sourceDoc.company_name, sourceDoc.period, parsed_indicator.indicator_id, convertIsDisclosed(parsed_indicator.isDisclosed),
                parsed_indicator.value[:2999], parsed_indicator.unit[:254], parsed_indicator.page_number[:200],
                createDocumentName(sourceDoc), parsed_indicator.section[:2999], response_metadata.cached_content_token_count,
                response_metadata.total_token_count, thoughts[:4999], elapsed_time)
        mycursor.execute(sql, val)

        mydb.commit()
    except (IntegrityError, MySQLInterfaceError) as e:
        if parsed_indicator.isDisclosed == 1:
            print(f"Updating {parsed_indicator.indicator_id} ")

            sql = ("UPDATE extraction_attempt3_test_unconsolidated "
                   "SET not_disclosed = %s, value = %s, unit = %s, pagenumber = %s, source_title = %s, text_section = %s, cached_content_token_count = %s, total_token_count = %s, thought_summary = %s, elapsed_time = %s "
                   "WHERE company_name = %s AND year = %s AND indicator_id = %s")
            val = (convertIsDisclosed(parsed_indicator.isDisclosed), parsed_indicator.value[:2999], parsed_indicator.unit,
                   parsed_indicator.page_number, createDocumentName(sourceDoc), parsed_indicator.section[:2999],
                   response_metadata.cached_content_token_count, response_metadata.total_token_count, thoughts[:4999], elapsed_time,
                   sourceDoc.company_name, sourceDoc.period, parsed_indicator.indicator_id)
            mycursor.execute(sql, val)

            mydb.commit()


    #print(mycursor.rowcount, f"{createDocumentName(sourceDoc)} record inserted.")

def insertIntoBatchMetricExtraction(responseData_dic):
    try:
        sql = ("INSERT INTO big_dataset_unconsolidated (company_name, year, indicator_id, not_disclosed, value, "
               "unit, pagenumber, source_title, text_section, input_token_count, output_token_count, thought_summary)"
               " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        val = (responseData_dic['company_name'], responseData_dic['year'], responseData_dic['indicator_id'],
               convertIsDisclosed(responseData_dic['isDisclosed']),
               responseData_dic['value'][:2999], responseData_dic['unit'][:254], responseData_dic['page_number'][:254],
               responseData_dic['source_title'], responseData_dic['section'][:2999], responseData_dic['inputTokenCount'],
               responseData_dic['outputTokenCount'], responseData_dic['thoughts'][:4999])
        mycursor.execute(sql, val)
        mydb.commit()
    except (IntegrityError, MySQLInterfaceError) as e:
        print(f"Updating {responseData_dic['company_name']} {responseData_dic['year']} {responseData_dic['indicator_id']} {responseData_dic['source_title']} ")
        sql = ("UPDATE big_dataset_unconsolidated "
               "SET company_name = %s, year = %s, indicator_id = %s, not_disclosed = %s, value = %s, unit = %s, pagenumber = %s, "
               "source_title = %s, text_section = %s, input_token_count = %s, output_token_count = %s, thought_summary = %s "
               "WHERE company_name = %s AND year = %s AND indicator_id = %s AND source_title = %s")
        val = (responseData_dic['company_name'], responseData_dic['year'], responseData_dic['indicator_id'],
               convertIsDisclosed(responseData_dic['isDisclosed']),
               responseData_dic['value'][:2999], responseData_dic['unit'][:254], responseData_dic['page_number'][:254],
               responseData_dic['source_title'], responseData_dic['section'][:2999], responseData_dic['inputTokenCount'],
               responseData_dic['outputTokenCount'], responseData_dic['thoughts'][:4999],
               responseData_dic['company_name'], responseData_dic['year'], responseData_dic['indicator_id'], responseData_dic['source_title'])
        mycursor.execute(sql, val)

        mydb.commit()


def select_communication_units(indicator_id, company_name, period):
    sql_query = "SELECT * FROM communicationunits_test WHERE company_name = %s AND year = %s AND indicator_id = %s"
    val = company_name, period, indicator_id
    mycursor.execute(sql_query, val)
    results = mycursor.fetchall()

    return list(results)
