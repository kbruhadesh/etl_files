from flask import Flask
from google.cloud import storage, bigquery

app = Flask(__name__)

def to_int_safe(value):
    try:
        if value is None or value.strip() == "":
            return None
        return int(float(value))
    except:
        return None

def to_float_safe(value):
    try:
        if value is None or value.strip() == "":
            return None
        return float(value)
    except:
        return None

@app.route("/", methods=["GET"])
def run_etl():
    client = storage.Client()
    bucket = client.bucket("etl-ecom-raw-etl-ecom-demo-479011")
    blob = bucket.blob("raw/dataset.csv")

    text = blob.download_as_text()
    lines = text.splitlines()
    header = lines[0].split(",")

    rows = []
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) != len(header):
            continue

        row = dict(zip(header, parts))

        # SAFE conversions
        row["Quantity"] = to_int_safe(row["Quantity"])
        row["Price"] = to_float_safe(row["Price"])
        row["CustomerID"] = to_int_safe(row["CustomerID"])

        rows.append(row)

    bq = bigquery.Client()
    table = bq.dataset("ecom_uk").table("retail_uk")
    bq.insert_rows_json(table, rows)

    return "ok"

if __name__ == "__main__":
    import os
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
