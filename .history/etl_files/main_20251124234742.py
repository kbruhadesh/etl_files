from flask import Flask, jsonify, make_response, request
from flask_cors import CORS
from google.cloud import bigquery

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Add CORS headers for every response
@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response

# Handle OPTIONS preflight
@app.route("/", methods=["OPTIONS"])
def options_handler():
    response = make_response(jsonify({"status": "ok"}), 200)
    return response

# Main ETL route (GET + POST)
@app.route("/", methods=["GET", "POST"])
def run_etl():
    try:
        client = bigquery.Client()

        table_id = "etl-ecom-demo-479011.ecom_uk.retail_uk"
        uri = "gs://etl-ecom-raw-etl-ecom-demo-479011/raw/dataset.csv"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE"
        )

        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()

        return make_response(
            jsonify({
                "success": True,
                "message": "SUCCESS: BigQuery Load Job Completed 🚀"
            }),
            200
        )

    except Exception as e:
        return make_response(jsonify({"success": False, "error": str(e)}), 500)

if __name__ == "__main__":
    import os
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

