from concurrent.futures import ProcessPoolExecutor, as_completed
from dbfread import DBF
from elasticsearch import Elasticsearch, helpers
from multiprocessing import Pool
import logging
import os
import time

from configs.database import ELASTIC_PASSWORD, ELASTIC_USERNAME, CHUNK_SIZE, ELASTICSEARCH_HOST, MAX_RETRIES, RETRY_DELAY, NUM_PROCESSES, ES_INDEX_NAME_PREFIX

# DBF directory path
dbf_directory = './data/bahia/PA_ACIMA_2008/teste'  # Specify the directory containing DBF files

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
        logging.debug(f"Preparing document for record: {record}")
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
        logging.debug(f"Yielding chunk starting from index {i}")
        yield record_list[i:i + chunk_size]

# Function to process a chunk of data and send it to Elasticsearch
def process_chunk(data_chunk, index_name):
    actions = [prepare_es_doc(record, index_name) for record in data_chunk if record]
    if actions:
        logging.info(f"Sending chunk of size {len(actions)} to Elasticsearch for index '{index_name}'.")
        for attempt in range(MAX_RETRIES):
            try:
                response = helpers.bulk(es, actions, chunk_size=CHUNK_SIZE)
                logging.info(f"Successfully indexed {len(actions)} documents to index '{index_name}'.")
                if response[0] < len(actions):
                    logging.error(f"Failed to index {len(actions) - response[0]} documents in chunk for index '{index_name}'.")
                break
            except Exception as e:
                logging.error(f"Error during bulk indexing to {index_name} (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)}")
                time.sleep(RETRY_DELAY)
        else:
            logging.error(f"Failed to index chunk of {len(actions)} documents after {MAX_RETRIES} attempts to index '{index_name}'.")
    else:
        logging.warning(f"No valid documents to index in this chunk for index '{index_name}'.")


# Function to handle parallel processing using multiprocessing.Pool
def parallel_bulk_index(dbf_directory):
    start_time = time.time()
    logging.info(f"Starting parallel indexing process.")

    dbf_files = [f for f in os.listdir(dbf_directory) if f.endswith('.dbf')]
    tasks = []
    
    with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        for dbf_file in dbf_files:
            index_name = f'{ES_INDEX_NAME_PREFIX}{os.path.splitext(dbf_file)[0]}'.lower().replace(' ', '_').replace('-', '_')
            try:
                logging.info(f"Reading DBF file: {dbf_file}")
                table = DBF(os.path.join(dbf_directory, dbf_file), encoding='latin-1')
                for data_chunk in chunk_records(table, INT_CHUNK_SIZE):
                    tasks.append(executor.submit(process_chunk, data_chunk, index_name))
            except Exception as e:
                logging.error(f"Failed to read DBF file {dbf_file}: {str(e)}")

        for future in as_completed(tasks):
            try:
                future.result()  # Will raise an exception if one occurred in `process_chunk`
            except Exception as exc:
                logging.error(f"An error occurred: {exc}")

    elapsed_time = time.time() - start_time
    logging.info(f"Data extraction and indexing took: {elapsed_time:.2f} seconds.")


# Start the parallel indexing process
if __name__ == "__main__":
    try:
        parallel_bulk_index(dbf_directory)
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
