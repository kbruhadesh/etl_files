from flask import Flask
from google.cloud import storage, bigquery

app = Flask(__name__)

@app.route("/", methods=["GET"])
def run_etl():
    client = storage.Client()
    bucket = client.bucket("etl-ecom-raw-etl-ecom-demo-479011")
    blob = bucket.blob("raw/dataset.csv")

    lines = blob.download_as_text().splitlines()
    header = [h.replace(" ", "") for h in lines[0].split(",")]


    rows = []
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) != len(header):
            continue
        rows.append(dict(zip(header, parts)))

    bq = bigquery.Client()
    table = bq.dataset("ecom_uk").table("retail_uk")
    bq.insert_rows_json(table, rows)

    return "ok"

if __name__ == "__main__":
    import os
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
