from flask import Flask
from google.cloud import storage, bigquery

app = Flask(__name__)

# ---------- SAFE CONVERTERS ----------
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


# ---------- ETL HANDLER ----------
@app.route("/", methods=["GET"])
def run_etl():

    # Read CSV from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket("etl-ecom-raw-etl-ecom-demo-479011")
    blob = bucket.blob("raw/dataset.csv")

    text = blob.download_as_text()
    lines = text.splitlines()

    header = lines[0].split(",")

    all_rows = []
    for line in lines[1:]:
        parts = line.split(",")

        if len(parts) != len(header):
            continue

        row = dict(zip(header, parts))

        row["Quantity"] = to_int_safe(row.get("Quantity"))
        row["Price"] = to_float_safe(row.get("Price"))
        row["CustomerID"] = to_int_safe(row.get("CustomerID"))

        all_rows.append(row)

    # ---------- LOAD TO BIGQUERY ----------
    bq = bigquery.Client()
    table = bq.dataset("ecom_uk").table("retail_uk")

    batch_size = 5000
    batch_errors = []

    for i in range(0, len(all_rows), batch_size):
        chunk = all_rows[i:i+batch_size]
        print(f"Inserting rows {i} to {i+len(chunk)} ...")

        errors = bq.insert_rows_json(table, chunk)
        if errors:
            batch_errors.append({"batch": i, "errors": errors})

    return {
        "status": "completed",
        "rows_inserted": len(all_rows),
        "batches": len(all_rows) // batch_size + 1,
        "errors": batch_errors
    }


# ---------- FLASK SERVER ----------
if __name__ == "__main__":
    import os
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
