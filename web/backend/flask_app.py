from flask import Flask, request, jsonify, make_response
from database_client import select_distinct, select_indicators, select_rows, select_industry_average, select_companies

app = Flask(__name__)

@app.route("/api/data", methods=["GET"])
def get_esg_data():
    industry = request.args.get("industry")
    companies = request.args.get("company")
    indicators = request.args.get("indicator_ids")
    years = request.args.get("years")
    selectUndisclosed = request.args.get("selectUndisclosed")

    industries = industry.split(",") if industry else []
    companies = companies.split(",") if companies else []
    indicators = indicators.split(",") if indicators else []
    years = years.split(",") if years else []

    if not industries and not companies:
        return make_cors_response([])

    if not indicators or not years:
        return make_cors_response([])

    selectUndisclosedBool = selectUndisclosed == "True"

    rows = select_rows(industries, companies, indicators, years, selectUndisclosedBool)
    return make_cors_response(rows)

def make_cors_response(data, status=200):
    response = make_response(jsonify(data), status)
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response

@app.route("/api/industries", methods=["GET"])
def get_industries():
    industries = select_distinct("industry")
    response = make_response(jsonify(industries), 200)
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response

@app.route("/api/companies", methods=["GET"])
def get_companies():
    companies = select_companies()
    response = make_response(jsonify(companies), 200)
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response

@app.route("/api/indicator_ids", methods=["GET"])
def get_indicator_ids():
    indicator_ids = select_indicators()
    response = make_response(jsonify(indicator_ids), 200)
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


@app.route("/api/industry_average/<industry_name>", methods=["GET"])
def get_industry_average(industry_name: str):
    indicator_id = request.args.get("indicator_id")
    years_param = request.args.get("years")

    if not indicator_id or not years_param:
        return make_cors_response([], 400)

    years = years_param.split(",")
    results = select_industry_average(industry_name, indicator_id, years)
    return make_cors_response(results)


if __name__ == '__main__':
    # Run the app locally on port 5000
    app.run(debug=True, port=5000)