import functions_framework
from google.cloud import storage, bigquery

@functions_framework.http
def run_etl(request):
    client = storage.Client()
    bucket = client.bucket("etl-ecom-raw-etl-ecom-demo-479011")
    blob = bucket.blob("raw/dataset.csv")
    data = blob.download_as_text().splitlines()

    header = data[0].split(",")
    rows = []

    for line in data[1:]:
        parts = line.split(",")
        if len(parts) != len(header):
            continue
        rows.append(dict(zip(header, parts)))

    bq = bigquery.Client()
    table = bq.dataset("ecom_uk").table("retail_uk")
    bq.insert_rows_json(table, rows)

    return "ok"
