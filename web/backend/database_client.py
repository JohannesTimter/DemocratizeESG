import mysql.connector


def select_distinct(columnname: str):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="MyN3wP4ssw0rd",
        database="democratizeesg"
    )

    mycursor = mydb.cursor()

    sql_string = f"SELECT DISTINCT {columnname} FROM big_dataset_consolidated_unit_converted3"
    mycursor.execute(sql_string)
    results = mycursor.fetchall()
    mycursor.close()

    values = []
    for row in results:
        values.append(row[0])

    return values

def select_indicators():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="MyN3wP4ssw0rd",
        database="democratizeesg"
    )

    mycursor = mydb.cursor()

    sql_string = f"SELECT * FROM democratizeesg_ekpis ORDER BY order_no ASC"
    mycursor.execute(sql_string)
    results = mycursor.fetchall()
    mycursor.close()

    return results


def select_rows(industries: list[str], companies: list[str], indicators: list[str], years: list[str], selectUndisclosed: bool):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="MyN3wP4ssw0rd",
        database="democratizeesg"
    )

    industries_quoted = [f'"{industry}"' for industry in industries]
    companies_quoted = [f'"{company}"' for company in companies]
    indicators_quoted = [f'"{indicator}"' for indicator in indicators]
    years_quoted = [f'"{year}"' for year in years]
    mycursor = mydb.cursor()

    industry_company_conditions = []
    if industries_quoted:
        industry_company_conditions.append(f"industry IN ({",".join(industries_quoted)})")
    if companies_quoted:
        industry_company_conditions.append(f"company_name IN ({",".join(companies_quoted)})")

    industry_company_sql = " OR ".join(industry_company_conditions)


    sql_string = (f"SELECT * "
                  f"FROM big_dataset_consolidated_unit_converted3 "
                  f"WHERE {industry_company_sql} "
                  f"AND year IN ({",".join(years_quoted)}) "
                  f"AND indicator_id IN ({",".join(indicators_quoted)})")

    if not selectUndisclosed:
        sql_string += f" AND not_disclosed = 0"

    mycursor.execute(sql_string)
    results = mycursor.fetchall()
    mycursor.close()

    return results

