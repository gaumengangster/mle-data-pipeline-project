import pandas as pd
from google.cloud import storage
from io import BytesIO

BUCKET_NAME = "mle-batch-real-time-process"
YEAR = 2025
MONTHS = [1, 2, 3]

def load_data_from_gcs(bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_as_bytes()
    return pd.read_parquet(BytesIO(data))

def process_data(df):
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["revenue"] = df["fare_amount"] + df["extra"] + df["mta_tax"] + df["tip_amount"] + df["tolls_amount"] + df["improvement_surcharge"]
    daily_revenue = df.groupby(df["lpep_pickup_datetime"].dt.date)["revenue"].sum().reset_index()
    daily_revenue.columns = ["date", "total_revenue"]
    return daily_revenue

def main():
    all_data = []
    for month in MONTHS:
        blob_name = f"green-taxi/{YEAR}/green_tripdata_{YEAR}-{month:02d}.parquet"
        print(f"📥 Processing {blob_name}")
        df = load_data_from_gcs(BUCKET_NAME, blob_name)
        daily_revenue = process_data(df)
        all_data.append(daily_revenue)
    
    result = pd.concat(all_data).groupby("date")["total_revenue"].sum().reset_index()
    result.to_csv("daily_revenue.csv", index=False)
    print("✅ Daily revenue calculation completed and saved to daily_revenue.csv")

if __name__ == "__main__":
    main()
