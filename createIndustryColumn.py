import pickle

from Fullcontext_main import retrieveCompanyYearReports
from Gemini import createBatchRequestJson
from GroundTruth import loadSheet
groundtruth_sheet_id = '18HCMbUmXcK9N2d4GziwUrHUgEHnc81ZH_4v-r-uJKcI' #Sheet with list of groundtruth sheets
big_dataset_range = "BigDataset!A1:C"
import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="MyN3wP4ssw0rd",
  database="democratizeesg"
)

mycursor = mydb.cursor()

def main():
    todo_list = selectCompanyYearsWithError()
    reports_to_reporocess = []

    with open('companyYearReports.pkl', 'rb') as f:
        # Load the object from the file
        reports_to_reporocess = pickle.load(f)



    #for todo in todo_list:
    #    companyYearReports = retrieveCompanyYearReports(todo[0], todo[1], todo[2])
    #    for companyYearReport in companyYearReports:
    #        if (companyYearReport.topic == "ESG" and todo[3] == "ESG") or (companyYearReport.topic == "Financial" and todo[3] == "FINANCIAL") or (companyYearReport.topic == "Annual Report" and todo[3] == "ANNUAL_REPORT"):
    #            reports_to_reporocess.append(companyYearReport)
    #            print(f"Appended {companyYearReport.company_name} {companyYearReport.period} {companyYearReport.topic}")
    #
    #            with open('companyYearReports.pkl', 'wb') as file:
    #                # 3. Use pickle.dump() to write the object to the file
    #                pickle.dump(reports_to_reporocess, file)
    #                print(f"companyYearReports.pkl aktualisiert.")

    #print(reports_to_reporocess)



    createBatchRequestJson(reports_to_reporocess)


def selectCompanyYearsWithError():
    sql_query = "SELECT DISTINCT(source_title), industry FROM big_dataset_unconsolidated WHERE thought_summary LIKE %s"
    val = ["%Error:%"]
    mycursor.execute(sql_query, val)
    results = mycursor.fetchall()

    todo_list = []

    for row in results:
        split = row[0].split("_")
        company = split[0]
        year = split[1]
        topic = split[2]
        industry = row[1]

        if [industry, company, year, topic] not in todo_list:
            todo_list.append([industry, company, year, topic])

    return todo_list

if __name__ == "__main__":
    main()