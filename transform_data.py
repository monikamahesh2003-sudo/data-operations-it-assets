"""
PHASE 3 — Data Transformation & Enrichment in Elasticsearch
Objective: Reindex, enrich, and clean data in Elasticsearch
"""

from elasticsearch import Elasticsearch

# ==============================
# 1. CONFIGURATION
# ==============================
CLOUD_URL = "https://elasticsearch-project-ed9b39.es.us-central1.gcp.elastic.cloud:443"
API_KEY = "T0VDZFNab0ItN3lhVUE2dlkzaUU6aTduR1k3TGtydlFUbkVKTWZqYm8zZw=="   # Replace with your own key

SOURCE_INDEX = "it-assets"         # existing index from Phase 2
TARGET_INDEX = "it-assets-enriched"  # new index for transformed data

# Connect to Elasticsearch
es = Elasticsearch(CLOUD_URL, api_key=API_KEY)

if es.ping():
    print("✅ Connected to Elasticsearch Cloud!")
else:
    print("❌ Could not connect. Check credentials or URL.")
    exit()

# ==============================
# 2. REINDEX DATA
# ==============================
print(f"🚀 Reindexing data from '{SOURCE_INDEX}' → '{TARGET_INDEX}'...")

mappings = {
    "mappings": {
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
            "risk_level": {"type": "keyword"},
            "system_age": {"type": "float"},
        }
    }
}


if not es.indices.exists(index=TARGET_INDEX):
    es.indices.create(index=TARGET_INDEX, body=mappings)
else:
    print("Destination index already exists")
    # exit()

if False:
    try:
        reindex_body = {
            "source": {"index": SOURCE_INDEX},
            "dest": {"index": TARGET_INDEX}
        }
        a = es.reindex(body=reindex_body, wait_for_completion=True, refresh=True)
        print("✅ Reindexing completed successfully!", a)
        # exit()
    except Exception as e:
        print("❌ Reindexing failed:", e)
        exit()

# ==============================
# 3. ADD DERIVED FIELD — risk_level
# ==============================
print("⚙️ Adding 'risk_level' field based on lifecycle status...")

risk_script = {
    "script": {
        "source": """
            if (ctx._source.containsKey('operating_system_lifecycle_status')) {
                String status = ctx._source.operating_system_lifecycle_status.toLowerCase();
                if (status == 'EOL' || status == 'EOS') {
                    ctx._source.risk_level = 'High';
                } else {
                    ctx._source.risk_level = 'Low';
                }
            } else {
                ctx._source.risk_level = 'Unknown';
            }
        """,
        "lang": "painless"
    },
    "query": {"match_all": {}}
}

try:
    es.update_by_query(index=TARGET_INDEX, body=risk_script, conflicts="proceed", refresh=True)
    print("✅ Added 'risk_level' field to all records!")
except Exception as e:
    print("❌ Error adding 'risk_level':", e)

exit()
# ==============================
# 4. CALCULATE SYSTEM AGE (in years)
# ==============================
print("⚙️ Calculating 'system_age' from installation date...")

system_age_script = {
    "script": {
        "source": """
            if (ctx._source.containsKey('operating_system_installation_date') && ctx._source.operating_system_installation_date != null) {
                try {
                    def date = LocalDate.parse(ctx._source.operating_system_installation_date);
                    def years = Period.between(date, LocalDate.now()).getYears();
                    ctx._source.system_age = years;
                } catch (Exception e) {
                    ctx._source.system_age = null;
                }
            }
        """,
        "lang": "painless"
    },
    "query": {"match_all": {}}
}

try:
    es.update_by_query(index=TARGET_INDEX, body=system_age_script, conflicts="proceed", refresh=True)
    print("✅ Added 'system_age' field successfully!")
except Exception as e:
    print("❌ Error calculating 'system_age':", e)

# ==============================
# 5. DELETE INVALID RECORDS
# ==============================
print("🧹 Deleting invalid records (missing hostnames or Unknown providers)...")

delete_query = {
    "query": {
        "bool": {
            "should": [
                {"bool": {"must_not": {"exists": {"field": "hostname"}}}},
                {"term": {"provider.keyword": "Unknown"}}
            ],
            "minimum_should_match": 1
        }
    }
}

try:
    es.delete_by_query(index=TARGET_INDEX, body=delete_query, conflicts="proceed", refresh=True)
    print("✅ Deleted invalid records successfully!")
except Exception as e:
    print("❌ Error deleting invalid records:", e)

# ==============================
# 6. SUMMARY
# ==============================
count = es.count(index=TARGET_INDEX)["count"]
print(f"📊 Transformation complete — final document count: {count}")

