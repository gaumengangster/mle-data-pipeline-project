from google.cloud import storage

client = storage.Client.from_service_account_json("google_key/mle-project-big-data-c94e28c0f632.json")

# Test listing buckets
buckets = list(client.list_buckets())
print("✅ Connected successfully! Buckets:")
for b in buckets:
    print("-", b.name)
