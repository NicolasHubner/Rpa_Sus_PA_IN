import concurrent.futures
from elasticsearch import Elasticsearch

# -------- CONFIG --------
ELASTICSEARCH_HOST = "https://localhost:9200"
ELASTIC_USERNAME = "elastic"
ELASTIC_PASSWORD = "nicolasprojetoflavio22"

NUM_INDICES = 165       # Number of latest indices to process
MAX_INDICES_CONCURRENT = 12  # How many indices to process simultaneously
SLICE_COUNT = 4         # Number of slices per index
REQUEST_TIMEOUT = 120
MAX_RETRIES = 5
# ------------------------

# Connect to Elasticsearch
es = Elasticsearch(
    [ELASTICSEARCH_HOST],
    basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
    verify_certs=False,
    retry_on_timeout=True,
    max_retries=MAX_RETRIES,
).options(request_timeout=REQUEST_TIMEOUT)

# Step 1: Get all indices sorted by creation date
all_indices = es.cat.indices(format="json", h=["index", "creation.date"])
latest_indices = sorted(all_indices, key=lambda x: int(x["creation.date"]), reverse=True)[:NUM_INDICES]
indices_to_reindex = [idx["index"] for idx in latest_indices]

print(f"Preparing to reindex {len(indices_to_reindex)} indices...")

# Step 2: Define function to reindex one slice
def reindex_slice(old_index, new_index, slice_id, max_slices=SLICE_COUNT):
    body = {
        "source": {"index": old_index, "slice": {"id": slice_id, "max": max_slices}},
        "dest": {"index": new_index},
    }
    return es.reindex(body=body, wait_for_completion=True)

# Step 3: Reindex one full index (all slices in parallel)
def reindex_index(old_index):
    new_index = f"{old_index}_sao_paulo"
    print(f"Starting reindex for {old_index} -> {new_index}")
    with concurrent.futures.ThreadPoolExecutor(max_workers=SLICE_COUNT) as executor:
        futures = [executor.submit(reindex_slice, old_index, new_index, i) for i in range(SLICE_COUNT)]
        for f in concurrent.futures.as_completed(futures):
            try:
                result = f.result()
                print(f"Slice done for {old_index}: {result}")
            except Exception as e:
                print(f"Error in slice for {old_index}: {e}")
    print(f"Completed reindex for {old_index} -> {new_index}")

# Step 4: Process multiple indices concurrently
with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_INDICES_CONCURRENT) as executor:
    executor.map(reindex_index, indices_to_reindex)

print("All reindex operations completed.")
