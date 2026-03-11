from flask import Flask, request, jsonify, make_response
from database_client import select_distinct, select_indicators
from web.backend.database_client import select_rows

app = Flask(__name__)

@app.route("/api/data", methods=["GET"])
def get_esg_data():
    #industry = request.args.get("industry").split(",")
    companies = request.args.get("company").split(",")
    indicators = request.args.get("indicator_ids").split(",")
    years = request.args.get("years").split(",")
    selectUndisclosed = request.args.get("selectUndisclosed")

    selectUndisclosedBool = True if selectUndisclosed == "True" else False


    rows = select_rows(companies, indicators, years, selectUndisclosedBool)
    response = make_response(jsonify(rows), 200)
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
    companies = select_distinct("company_name")
    response = make_response(jsonify(companies), 200)
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response

@app.route("/api/indicator_ids", methods=["GET"])
def get_indicator_ids():
    indicator_ids = select_indicators()
    response = make_response(jsonify(indicator_ids), 200)
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


if __name__ == '__main__':
    # Run the app locally on port 5000
    app.run(debug=True, port=5000)