from flask import Flask, request, jsonify
from database_client import select_distinct
app = Flask(__name__)

@app.route("/api/data", methods=["GET"])
def get_esg_data():
    industry = request.args.get("industry")
    company = request.args.get("company")
    years = request.args.get("years")
    start_year, end_year = None, None

    if years:
        try:
            start_year_str, end_year_str = years.split("-")
            start_year = int(start_year_str)
            end_year = int(end_year_str)
        except ValueError:
            return jsonify({"error": "Invalid year format"}), 400

    indicators_param = request.args.get("indicator_ids")
    indicator_ids = None
    if indicators_param:
        indicator_ids = indicators_param.split(",")

    return jsonify({
        "industry": industry,
        "company": company,
        "start_year": start_year,
        "end_year": end_year,
        "indicator_ids": indicator_ids
    }), 200

@app.route("/api/industries", methods=["GET"])
def get_industries():
    industries = select_distinct("industry")
    return jsonify(industries), 200

@app.route("/api/companies", methods=["GET"])
def get_companies():
    companies = select_distinct("company_name")
    return jsonify(companies), 200

@app.route("/api/indicator_ids", methods=["GET"])
def get_indicator_ids():
    indicator_ids = select_distinct("indicator_id")
    return jsonify(indicator_ids), 200


if __name__ == '__main__':
    # Run the app locally on port 5000
    app.run(debug=True, port=5000)