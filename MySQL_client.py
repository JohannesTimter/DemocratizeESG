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