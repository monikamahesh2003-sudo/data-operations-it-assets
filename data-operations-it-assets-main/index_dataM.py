import pandas as pd
from elasticsearch import Elasticsearch, helpers

# === CONFIGURATION ===
ES_ENDPOINT = "https://elasticsearch-fd32ae.es.us-central1.gcp.elastic.cloud:443"
ES_API_KEY = "NDEtMVNwb0JCdVFscW5fRHZteV86YnpoUl9iMTRzWVhrUUlSamJlVDF0dw=="
CSV_PATH = "it_asset_inventory_cleaned.csv"
INDEX_NAME = "it_assets_inventory_cleaned"

# === CONNECT TO ELASTIC ===
es = Elasticsearch(
	ES_ENDPOINT,
	api_key=ES_API_KEY,
	verify_certs=True
)

if not es.ping():
	print(" Connection failed! Please check endpoint or API key.")
	exit()
else:
	print(" Connected to Elasticsearch!")

# === READ CSV ===
df = pd.read_csv(CSV_PATH)
print(f"Read {len(df)} rows from {CSV_PATH}")

# === PREPARE ACTIONS FOR BULK API ===
actions = [
	{
		"_index": INDEX_NAME,
		"_source": row.dropna().to_dict()
	}
	for _, row in df.iterrows()
]

print(f"Uploading {len(actions)} documents to index '{INDEX_NAME}'...")

# === BULK UPLOAD ===
success, failed = helpers.bulk(es, actions, stats_only=True)
print(f" Uploaded: {success}, Failed: {failed}")
