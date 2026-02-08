from flask import Flask
from flask_cors import CORS
from google.cloud import bigquery

app = Flask(__name__)
CORS(app)

@app.route("/", methods=["GET"])
def run_etl():
    client = bigquery.Client()

    table_id = "etl-ecom-demo-479011.ecom_uk.retail_uk"
    uri = "gs://etl-ecom-raw-etl-ecom-demo-479011/raw/dataset.csv"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )

    load_job.result()

    return "SUCCESS: Data loaded using BigQuery Load Job 🚀"
