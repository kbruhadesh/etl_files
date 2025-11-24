from flask import Flask
from google.cloud import storage, bigquery

app = Flask(__name__)

def to_int_safe(value):
    try:
        if value is None:
            return None
        value = value.strip()
        if value == "" or value.lower() in ("nan", "none", "null"):
            return None
        return int(float(value))
    except:
        return None

def to_float_safe(value):
    try:
        if value is None:
            return None
        value = value.strip()
        if value == "" or value.lower() in ("nan", "none", "null"):
            return None
        return float(value)
    except:
        return None


@app.route("/run", methods=["GET"])
def run_etl():

    storage_client = storage.Client()
    bucket = storage_client.bucket("etl-ecom-raw-etl-ecom-demo-479011")
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
        row["Quantity"] = to_int_safe(row.get("Quantity"))
        row["Price"] = to_float_safe(row.get("Price"))
        row["CustomerID"] = to_int_safe(row.get("CustomerID"))
        rows.append(row)

    bq = bigquery.Client()
    table = bq.dataset("ecom_uk").table("retail_uk")

    def chunker(seq, size):
        for pos in range(0, len(seq), size):
            yield seq[pos:pos + size]

    errors_all = []
    for batch in chunker(rows, 5000):
        errors = bq.insert_rows_json(table, batch)
        if errors:
            errors_all.extend(errors)

    return str(errors_all)


if __name__ == "__main__":
    import os
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
