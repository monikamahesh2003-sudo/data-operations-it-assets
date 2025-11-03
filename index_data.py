from elasticsearch import Elasticsearch, helpers
import csv 
 
CLOUD_URL = "https://elasticsearch-project-ed9b39.es.us-central1.gcp.elastic.cloud:443"  # Replace with your Cloud endpoint
API_KEY = "T0VDZFNab0ItN3lhVUE2dlkzaUU6aTduR1k3TGtydlFUbkVKTWZqYm8zZw=="  # Replace with your actual API key
EXCEL_FILE = "it_asset_inventory_cleaned.csv"
INDEX_NAME = "it-assets"
 
 
es = Elasticsearch(
    CLOUD_URL,
    api_key=API_KEY
)
 
# Test connection
if es.ping():
    print("Connected to Elasticsearch Cloud!")
else:
    print("Could not connect. Check your CLOUD_URL or API_KEY.")
    exit()

all_data = []

# Loading data from csv to python as dictionary
with open("it_asset_inventory_cleaned.csv") as f:
    data = csv.DictReader(f)
    for line in data:
        line["os_is_virtual"] = line["os_is_virtual"].lower() == "true"
        line["is_internet_facing"] = line["is_internet_facing"].lower() == "yes"
        line["performance_score"] = float(line["performance_score"])

        all_data.append(line)

mappings = {
    "properties": {
        "hostname": {"type": "keyword"},
        "country": {"type": "keyword"},
        "operating_system_name": {"type": "keyword"},
        "operating_system_provider": {"type": "keyword"},
        "operating_system_installation_date": {"type": "date"},
        "operating_system_lifecycle_status": {"type": "keyword"},
        "os_is_virtual": {"type": "boolean"},
        "is_internet_facing": {"type": "boolean"},
        "image_purpose": {"type": "keyword"},
        "os_system_id": {"type": "keyword"},
        "performance_score": {"type": "float"},
    }
}

es.indices.put_mapping(index=INDEX_NAME, body=mappings)

ingestion_timeout = 300
bulk_response = helpers.bulk(
    es.options(request_timeout=ingestion_timeout), all_data, index=INDEX_NAME
)
print(bulk_response)
