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

    # ---------- BATCH INSERT ----------
    bq = bigquery.Client()
    table = bq.dataset("ecom_uk").table("retail_uk")

    batch_size = 500
    all_errors = []

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        errors = bq.insert_rows_json(table, batch)
        if errors:
            all_errors.extend(errors)

    return {
        "rows_inserted": len(rows),
        "errors_found": len(all_errors),
        "errors": all_errors
    }


if __name__ == "__main__":
    import os
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
