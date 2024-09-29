from dbfread import DBF
from elasticsearch import Elasticsearch, helpers
from multiprocessing import Pool
import logging
import time

from configs.database import es, CHUNK_SIZE, ES_INDEX_NAME_PREFIX, MAX_RETRIES, NUM_PROCESSES, RETRY_DELAY

# DBF file path
dbf_file = 'PA_2009.dbf'

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to process a single record into Elasticsearch document format
def prepare_es_doc(record):
    return {
        '_index': ES_INDEX_NAME_PREFIX + dbf_file,
        '_source': dict(record)  # Convert DBF record to dictionary
    }

# Function to chunk records into smaller batches
def chunk_records(records, chunk_size=CHUNK_SIZE):
    """Generator that yields chunks of records from the DBF table."""
    record_list = list(records)
    for i in range(0, len(record_list), chunk_size):
        yield record_list[i:i + chunk_size]

# Function to process a chunk of data and send it to Elasticsearch
def process_chunk(data_chunk):
    """Indexes a chunk of data into Elasticsearch with retries."""
    actions = [prepare_es_doc(record) for record in data_chunk]
    for attempt in range(MAX_RETRIES):  # Retry up to MAX_RETRIES times
        try:
            helpers.bulk(es, actions, chunk_size=CHUNK_SIZE)
            logging.info(f"Indexed {len(data_chunk)} documents successfully.")
            break  # Exit loop if successful
        except Exception as e:
            logging.error(f"Error during bulk indexing (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)}")
            time.sleep(RETRY_DELAY)
    else:
        logging.error(f"Failed to index {len(data_chunk)} documents after {MAX_RETRIES} attempts.")

# Function to handle parallel processing using multiprocessing.Pool
def parallel_bulk_index(dbf_file, num_processes=NUM_PROCESSES):
    """Extracts data from DBF, splits it into chunks, and indexes using multiprocessing."""
    start_time = time.time()

    # Read the DBF file
    table = DBF(dbf_file)

    # Use Pool to parallelize the processing of chunks
    with Pool(processes=num_processes) as pool:
        # Send chunks to the pool for parallel processing
        pool.map(process_chunk, chunk_records(table, CHUNK_SIZE))

    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f"Data extraction and indexing took: {elapsed_time:.2f} seconds")

# Start the parallel indexing process
if __name__ == "__main__":
    try:
        parallel_bulk_index(dbf_file)
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
