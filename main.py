from flask import Flask, request
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd

app = Flask(__name__)

@app.route("/", methods=["POST"])
def process_file():
    data = request.get_json()

    bucket_name = data['bucket']
    file_name = data['name']

    # Read from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    file_path = f"/tmp/{file_name}"
    blob.download_to_filename(file_path)

    # Read CSV
    df = pd.read_csv(file_path)

    # Simple cleaning
    df = df.dropna()

    # Load to BigQuery
    bq_client = bigquery.Client()
    table_id = "your_project.your_dataset.demo_table"

    job = bq_client.load_table_from_dataframe(df, table_id)
    job.result()

    return "Loaded successfully", 200