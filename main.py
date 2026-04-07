from flask import Flask, request, jsonify
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import os

app = Flask(__name__)

# Health check (required for Cloud Run)
@app.route("/", methods=["GET"])
def health():
    return "Service is running 🚀", 200


# Main processing endpoint
@app.route("/", methods=["POST"])
def process_file():
    try:
        data = request.get_json()

        if not data or "bucket" not in data or "name" not in data:
            return jsonify({"error": "Invalid request"}), 400

        bucket_name = data["bucket"]
        file_name = data["name"]

        # Initialize clients
        storage_client = storage.Client()
        bq_client = bigquery.Client()

        # Download file from GCS
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        local_file = f"/tmp/{file_name}"
        blob.download_to_filename(local_file)

        # Read CSV
        df = pd.read_csv(local_file)

        # Basic cleaning
        df = df.dropna()

        # Replace with your actual project/dataset/table
        table_id = "qwiklabs-gcp-04-99a2b556d735.demo_dataset.demo_table"

        # Load to BigQuery
        job = bq_client.load_table_from_dataframe(df, table_id)
        job.result()

        return jsonify({
            "status": "success",
            "rows_loaded": len(df)
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# Only needed if NOT using gunicorn (safe to keep)
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)