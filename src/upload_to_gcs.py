import os
import requests
from google.cloud import storage

# Configuration
BUCKET_NAME = "mle-batch-real-time-process"
YEAR = 2025
MONTHS = [1, 2, 3]
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def upload_to_gcs(bucket_name, source_file, destination_blob):
    """Upload a file to a GCS bucket."""
    client = storage.Client.from_service_account_json("google_key/mle-project-big-data-c94e28c0f632.json")
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)
    print(f"✅ Uploaded {source_file} to gs://{bucket_name}/{destination_blob}")

def main():
    for month in MONTHS:
        month_str = f"{month:02d}"
        file_name = f"green_tripdata_{YEAR}-{month_str}.parquet"
        url = f"{BASE_URL}/{file_name}"
        
        # Download the file
        print(f"⬇️ Downloading {url}...")
        response = requests.get(url)
        local_path = f"./{file_name}"
        with open(local_path, "wb") as f:
            f.write(response.content)

        # Upload to GCS
        upload_to_gcs(BUCKET_NAME, local_path, f"green-taxi/{YEAR}/{file_name}")

        # Optionally delete local file
        os.remove(local_path)

if __name__ == "__main__":
    main()
