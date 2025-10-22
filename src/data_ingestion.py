import click
import pandas as pd
import os
from google.cloud import storage
from io import BytesIO

@click.command()
@click.option('--project_id', required=True, help='Project ID of your GCP project')
@click.option('--year', default=2024, show_default=True, help='Year to download data for')
@click.option('--bucket', required=True, help='Name of the GCS bucket to upload the data to')
@click.option('--color', default='yellow', show_default=True, help='Taxi type (e.g., yellow or green)')
def data_ingestion(project_id, year, bucket, color):
    """
    Downloads the NYC taxi trip data for the first 3 months of the given year
    and uploads them to the specified GCS bucket.
    """

    # Hardcoded path to your service account key
    sa_path = "google_key/mle-project-big-data-c94e28c0f632.json"

    # Set environment variable for authentication
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path

    # Initialize GCS client
    client = storage.Client(project=project_id)
    gcs_bucket = client.get_bucket(bucket)

    for month in range(1, 4):
        file_name = f"{color}_tripdata_{year}-{month:02d}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

        print(f"⬇️ Downloading data from {url} ...")
        df_taxi = pd.read_parquet(url)

        print(f"☁️ Uploading {file_name} to GCS ...")
        
        # Write DataFrame to an in-memory bytes buffer
        parquet_buffer = BytesIO()
        df_taxi.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        # Upload from bytes buffer
        blob = gcs_bucket.blob(f"{color}_taxi/{year}/{file_name}")
        blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")

        print(f"✅ Successfully uploaded: gs://{bucket}/{color}_taxi/{year}/{file_name}")


if __name__ == '__main__':
    data_ingestion()


def run_data_ingestion(project_id, bucket, color='yellow', year=2025):
    """
    Downloads the first 3 months of NYC taxi trip data and uploads to GCS.
    """

    # Hardcoded service account path
    sa_path = os.path.join(os.path.dirname(__file__), "google_key/mle-project-big-data-c94e28c0f632.json")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path

    client = storage.Client(project=project_id)
    gcs_bucket = client.get_bucket(bucket)

    for month in range(1, 4):
        file_name = f"{color}_tripdata_{year}-{month:02d}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

        print(f"⬇️ Downloading data from {url} ...")
        df_taxi = pd.read_parquet(url)

        print(f"☁️ Uploading {file_name} to GCS ...")
        parquet_buffer = BytesIO()
        df_taxi.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        blob = gcs_bucket.blob(f"{color}_taxi/{year}/{file_name}")
        blob.upload_from_file(parquet_buffer, content_type="application/octet-stream")

        print(f"✅ Uploaded: gs://{bucket}/{color}_taxi/{year}/{file_name}")