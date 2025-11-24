from flask import Flask
from google.cloud import storage, bigquery
import time

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

# ---------- ETL ----------
@app.route("/", methods=["GET"])
def run_etl():

    # ---- READ CSV FROM GCS ----
    storage_client = storage.Client()
    bucket = storage_client.bucket("etl-ecom-raw-etl-ecom-demo-479011")
    blob = bucket.blob("raw/dataset.csv")

    text = blob.download_as_text()
    lines = text.splitlines()

    # Clean header
    header = [h.strip().replace("\ufeff", "") for h in lines[0].split(",")]

    rows = []
    for line in lines[1:]:
        parts = [p.strip() for p in line.split(",")]

        if len(parts) != len(header):
            continue

        row = dict(zip(header, parts))

        # SAFE conversions
        row["Quantity"] = to_int_safe(row.get("Quantity"))
        row["Price"] = to_float_safe(row.get("Price"))
        row["CustomerID"] = to_int_safe(row.get("CustomerID"))

        rows.append(row)

    # ---- LOAD INTO BIGQUERY (BATCHED + RETRIES) ----
    bq = bigquery.Client()
    table = bq.dataset("ecom_uk").table("retail_uk")

    BATCH_SIZE = 500      # even safer
    MAX_RETRIES = 5
    all_errors = []

    for start in range(0, len(rows), BATCH_SIZE):
        batch = rows[start:start + BATCH_SIZE]

        for attempt in range(MAX_RETRIES):
            errors = bq.insert_rows_json(table, batch)

            if not errors:  # success
                break

            time.sleep(1 + attempt)  # exponential backoff

            if attempt == MAX_RETRIES - 1:
                all_errors.append({f"batch_{start//BATCH_SIZE}": errors})

    if not all_errors:
        return "SUCCESS: All rows inserted without errors."

    return str(all_errors)


# ------ CLOUD RUN SERVER ------
if __name__ == "__main__":
    import os
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
