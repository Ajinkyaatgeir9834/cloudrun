from flask import Flask, request, jsonify
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import os
import base64
import json

app = Flask(__name__)

# Health check
@app.route("/", methods=["GET"])
def health():
    return "Service is running 🚀", 200


# Main processing endpoint
@app.route("/", methods=["POST"])
def process_file():
    try:
        data = request.get_json()

        # 🔹 Decode Pub/Sub message
        if not data or "message" not in data:
            return jsonify({"error": "Invalid Pub/Sub message"}), 400

        pubsub_message = base64.b64decode(data["message"]["data"]).decode("utf-8")
        message_json = json.loads(pubsub_message)

        bucket_name = message_json["bucket"]
        file_name = message_json["name"]

        # Only process CSV files
        if not file_name.endswith(".csv"):
            return jsonify({"status": "skipped", "reason": "Not a CSV file"}), 200

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

        table_id = "qwiklabs-gcp-04-99a2b556d735.demo_dataset.demo_table"

        # Load to BigQuery
        job = bq_client.load_table_from_dataframe(df, table_id)
        job.result()

        return jsonify({
            "status": "success",
            "file": file_name,
            "rows_loaded": len(df)
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)