from elasticsearch import Elasticsearch
import time

# -------- CONFIG --------
ELASTICSEARCH_HOST = "https://localhost:9200"  # your ES endpoint
ELASTIC_USERNAME = "elastic"
ELASTIC_PASSWORD = "nicolasprojetoflavio22"
MAX_RETRIES = 5
REQUEST_TIMEOUT = 120
NUM_INDICES = 165  # number of latest indices to process
# ------------------------

# Connect to Elasticsearch
es = Elasticsearch(
    [ELASTICSEARCH_HOST],
    basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
    verify_certs=True,
    retry_on_timeout=True,
    max_retries=MAX_RETRIES,
).options(request_timeout=REQUEST_TIMEOUT)

# Step 1: Get all indices with creation date
all_indices = es.cat.indices(format="json", h=["index", "creation.date"])
# Sort by creation date descending and pick the latest 165
latest_indices = sorted(all_indices, key=lambda x: int(x["creation.date"]), reverse=True)[:NUM_INDICES]

print(f"Found {len(latest_indices)} indices to reindex.")

# Step 2: Reindex each index
for idx in latest_indices:
    old_index = idx["index"]
    new_index = f"{old_index}_sao_paulo"
    
    print(f"Reindexing {old_index} -> {new_index} ...")
    
    try:
        resp = es.reindex(
            body={
                "source": {"index": old_index},
                "dest": {"index": new_index}
            },
            wait_for_completion=True
        )
        print("Done:", resp)
    except Exception as e:
        print(f"Error reindexing {old_index}:", e)
        continue
    
    # Optional: short sleep to avoid overwhelming the cluster
    time.sleep(1)

print("All reindex operations completed.")
