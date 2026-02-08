from flask import Flask
from google.cloud import bigquery

app = Flask(__name__)

@app.route("/", methods=["GET"])
def run_etl():

    client = bigquery.Client()

    table_id = "etl-ecom-demo-479011.ecom_uk.retail_uk"
    uri = "gs://etl-ecom-raw-etl-ecom-demo-479011/raw/dataset.csv"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,    # infer types automatically
        write_disposition="WRITE_TRUNCATE"  # replace table every run
    )

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )

    load_job.result()  # Wait for job to finish

    return "SUCCESS: Data loaded using BigQuery Load Job 🚀"


if __name__ == "__main__":
    import os
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
