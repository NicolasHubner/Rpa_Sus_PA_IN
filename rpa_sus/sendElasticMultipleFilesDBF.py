from dbfread import DBF
from elasticsearch import Elasticsearch, helpers
from multiprocessing import Pool
import logging
import os
import time

from configs.database import ELASTIC_PASSWORD, ELASTIC_USERNAME, CHUNK_SIZE, ELASTICSEARCH_HOST, MAX_RETRIES, RETRY_DELAY, NUM_PROCESSES, ES_INDEX_NAME_PREFIX


# DBF directory path
dbf_directory = './data/RD_ACRE'  # Specify the directory containing DBF files

# Create Elasticsearch client
es = Elasticsearch(
    [ELASTICSEARCH_HOST],
    basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
).options(request_timeout=60)

INT_CHUNK_SIZE = int(CHUNK_SIZE)

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to process a single record into Elasticsearch document format
def prepare_es_doc(record, index_name):
    try:
        return {
            '_index': index_name,
            '_source': {k: str(v) if v is not None else None for k, v in record.items()}
        }
    except Exception as e:
        logging.error(f"Error preparing document for index {index_name}: {str(e)}")
        return None  # Skip this document

# Function to chunk records into smaller batches
def chunk_records(records, chunk_size=CHUNK_SIZE):
    """Generator that yields chunks of records from the DBF table."""
    record_list = list(records)
    for i in range(0, len(record_list), chunk_size):
        yield record_list[i:i + chunk_size]

# Function to process a chunk of data and send it to Elasticsearch
def process_chunk(data_chunk, index_name):
    """Indexes a chunk of data into Elasticsearch with retries."""
    actions = [prepare_es_doc(record, index_name) for record in data_chunk if record]  # Skip None records
    for attempt in range(MAX_RETRIES):
        try:
            # Use the helpers.bulk and capture the response
            response = helpers.bulk(es, actions, chunk_size=CHUNK_SIZE)
            logging.info(f"Indexed {len(data_chunk)} documents successfully.")
            if response[0] < len(actions):
                logging.error(f"Failed to index {len(actions) - response[0]} documents.")
            break
        except Exception as e:
            logging.error(f"Error during bulk indexing to {index_name} (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)}")
            time.sleep(RETRY_DELAY)
    else:
        logging.error(f"Failed to index {len(data_chunk)} documents after {MAX_RETRIES} attempts.")

# Function to handle parallel processing using multiprocessing.Pool
def parallel_bulk_index(dbf_directory, num_processes=NUM_PROCESSES):
    """Extracts data from multiple DBF files, splits it into chunks, and indexes using multiprocessing."""
    start_time = time.time()

    # List all DBF files in the directory
    dbf_files = [f for f in os.listdir(dbf_directory) if f.endswith('.dbf')]

    # Use Pool to parallelize the processing of chunks
    with Pool(processes=num_processes) as pool:
        for dbf_file in dbf_files:
            # Create a unique index name in lowercase and with underscores
            index_name = f'{ES_INDEX_NAME_PREFIX}{os.path.splitext(dbf_file)[0]}'.lower().replace(' ', '_').replace('-', '_')
            try:
                # Try different encodings
                table = DBF(os.path.join(dbf_directory, dbf_file), encoding='latin-1')  # or try 'windows-1252'
                # Send chunks to the pool for parallel processing
                pool.starmap(process_chunk, [(data_chunk, index_name) for data_chunk in chunk_records(table, INT_CHUNK_SIZE)])
            except Exception as e:
                logging.error(f"Failed to read DBF file {dbf_file}: {str(e)}")

    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f"Data extraction and indexing took: {elapsed_time:.2f} seconds")

# Start the parallel indexing process
if __name__ == "__main__":
    try:
        parallel_bulk_index(dbf_directory)
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
