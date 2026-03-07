import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="MyN3wP4ssw0rd",
  database="democratizeesg"
)

mycursor = mydb.cursor()

def select_distinct(columnname: str):
    sql_string = f"SELECT DISTINCT {columnname} FROM big_dataset_consolidated_unit_converted3"
    mycursor.execute(sql_string)
    results = mycursor.fetchall()

    values = []
    for row in results:
        values.append(row[0])

    return values