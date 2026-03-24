import mysql.connector

TABLE = "big_dataset_consolidated_unit_converted3"


def _get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="MyN3wP4ssw0rd",
        database="democratizeesg"
    )


def _query(sql: str, params: tuple = ()):
    """Execute a SQL query and return all results, closing the cursor afterwards."""
    conn = _get_connection()
    cursor = conn.cursor()
    cursor.execute(sql, params)
    results = cursor.fetchall()
    cursor.close()
    return results


def select_distinct(columnname: str):
    rows = _query(
        f"SELECT DISTINCT {columnname} FROM {TABLE} ORDER BY {columnname} ASC"
    )
    return [row[0] for row in rows]


def select_companies():
    return _query(
        f"SELECT DISTINCT company_name, industry FROM {TABLE} ORDER BY industry ASC"
    )


def select_indicators():
    return _query("SELECT * FROM democratizeesg_ekpis ORDER BY order_no ASC")


def select_rows(industries: list[str], companies: list[str], indicators: list[str], years: list[str], selectUndisclosed: bool):
    industries_quoted = [f'"{v}"' for v in industries]
    companies_quoted  = [f'"{v}"' for v in companies]
    indicators_quoted = [f'"{v}"' for v in indicators]
    years_quoted      = [f'"{v}"' for v in years]

    if industries_quoted and not companies_quoted:
        filter_sql = f"industry IN ({','.join(industries_quoted)}) AND"
    elif companies_quoted and not industries_quoted:
        filter_sql = f"company_name IN ({','.join(companies_quoted)}) AND"
    else:
        filter_sql = (f"(industry IN ({','.join(industries_quoted)}) "
                      f"OR company_name IN ({','.join(companies_quoted)})) AND")

    sql = (f"SELECT * FROM {TABLE} "
           f"WHERE {filter_sql} "
           f"year IN ({','.join(years_quoted)}) "
           f"AND indicator_id IN ({','.join(indicators_quoted)})")

    if not selectUndisclosed:
        sql += " AND not_disclosed = 0"

    sql += " LIMIT 1000"

    return _query(sql)


def select_industry_average(industry: str, indicator_id: str, years: list[str]):
    years_quoted = [f'"{y}"' for y in years]

    sql = (f"SELECT year, AVG(value) FROM {TABLE} "
           f"WHERE industry = %s "
           f"AND indicator_id = %s "
           f"AND year IN ({','.join(years_quoted)}) "
           f"AND not_disclosed = 0 "
           f"GROUP BY year ORDER BY year ASC")

    rows = _query(sql, (industry, indicator_id))
    return [[str(row[0]), float(row[1])] for row in rows if row[1] is not None]
