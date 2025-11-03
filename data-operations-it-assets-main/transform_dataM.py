import pandas as pd
from elasticsearch import Elasticsearch, helpers
 
# === CONFIGURATION ===
ES_ENDPOINT = "https://elasticsearch-fd32ae.es.us-central1.gcp.elastic.cloud:443"
ES_API_KEY = "NDEtMVNwb0JCdVFscW5fRHZteV86YnpoUl9iMTRzWVhrUUlSamJlVDF0dw=="
CSV_PATH = "it_asset_inventory_cleaned.csv"
INDEX_NAME = "it_assets_inventory_cleaned_transform"
 
# === CONNECT TO ELASTIC ===
es = Elasticsearch(
    ES_ENDPOINT,
    api_key=ES_API_KEY,
    verify_certs=True
)
 
if not es.ping():
    print("❌ Connection failed! Please check endpoint or API key.")
    exit()
else:
    print("✅ Connected to Elasticsearch!")
 
# === READ CSV ===
df = pd.read_csv(CSV_PATH)
print(f"Read {len(df)} rows from {CSV_PATH}")
 
# Add derived field 'risk_level' (very basic and easy to understand)
risk_levels = []
for status in df['operating_system_lifecycle_status']:
    if str(status).strip().upper() in ['EOL', 'EOS']:
        risk_levels.append('High')
    else:
        risk_levels.append('Low')
df['risk_level'] = risk_levels
 
# Calculate system age in years from installation date
df['operating_system_installation_date'] = pd.to_datetime(df['operating_system_installation_date'], errors='coerce')
current_date = pd.Timestamp.today()
df['system_age_years'] = ((current_date - df['operating_system_installation_date']).dt.days / 365.25).round(2)
 
# Delete records with missing hostnames or where hostname is 'Unknown'
df = df[df['hostname'].notna() & (df['hostname'].str.strip() != '')]
df = df[df['hostname'].str.strip().str.upper() != 'UNKNOWN']
# df = df[df['operating_system_provider'].str.strip().str.upper() != 'UNKNOWN']
 
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
 
print(f"✅ Uploaded: {success}, Failed: {failed}")
 
# === UPDATE EXISTING RECORDS WITH NEW FIELDS ===
for _, row in df.iterrows():
    query = {
        "script": {
            "source": "ctx._source.risk_level = params.risk_level; ctx._source.system_age_years = params.system_age_years;",
            "lang": "painless",
            "params": {
                "risk_level": row['risk_level'],
                "system_age_years": row['system_age_years']
            }
        },
        "query": {
            "term": {"os_system_id.keyword": str(row['os_system_id'])}
        }
    }
    es.update_by_query(index=INDEX_NAME, body=query, conflicts="proceed")