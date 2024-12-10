from concurrent.futures import ProcessPoolExecutor, as_completed
from dbfread import DBF
from elasticsearch import Elasticsearch, helpers
import logging
import os
import time

from configs.database import ELASTICSEARCH_HOST, ES_INDEX_NAME_PREFIX, CHUNK_SIZE, MAX_RETRIES, RETRY_DELAY, NUM_PROCESSES, ELASTIC_USERNAME, ELASTIC_PASSWORD


# DBF directory path
dbf_directory = './data/pa_acre'  # Specify the directory containing DBF files

# dbf_directory = '/mnt/volume_nyc1_01/nicolas/alagoas/PA_ACIMA_2008'

# Create Elasticsearch client
es = Elasticsearch(
    [ELASTICSEARCH_HOST],
    basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
).options(request_timeout=60)

INT_CHUNK_SIZE = int(CHUNK_SIZE)

# Logging setup
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

COLUMNS_TO_WATCH = ["PA_CODUNI",
                    "PA_UFMUN",
                    "PA_CNPJCPF",
                    "PA_CNPJMNT",
                    "PA_CMP",
                    "PA_PROC_ID",
                    "PA_TPFIN",
                    "PA_SUBFIN",
                    "PA_AUTORIZ",
                    "PA_CIDPRI",
                    "PA_CIDSEC",
                    "PA_CATEND",
                    "PA_QTDPRO",
                    "PA_QTDAPR",
                    "PA_VALPRO",
                    "PA_VALAPR",
                    ]


def prepare_es_doc(record, index_name, fields_to_include=COLUMNS_TO_WATCH):
    # Filter fields based on the columns to watch
    if fields_to_include:
        record = {k: v for k, v in record.items() if k in fields_to_include}
    return {
        '_index': index_name,
        '_source': {k: str(v) if v is not None else None for k, v in record.items()},
    }


def read_in_batches(file_path, chunk_size):
    """Generator that yields chunks of records from the DBF table."""
    table = DBF(file_path, encoding='latin-1', ignore_missing_memofile=True)
    batch = []
    for record in table:
        batch.append(record)
        if len(batch) == chunk_size:
            yield batch
            batch = []  # Clear the batch after yielding
    if batch:  # Yield any remaining records in the final batch
        yield batch


# # Function to process a chunk of data and send it to Elasticsearch
def process_chunk(data_chunk, index_name):
    try:
        if not es.indices.exists(index_name):
            es.indices.create(index=index_name, body={
                "mappings": {
                    "properties": {
                        "PA_CODUNI": {"type": "keyword"},
                        "PA_UFMUN": {"type": "keyword"},
                        "PA_CNPJCPF": {"type": "keyword"},
                        "PA_CNPJMNT": {"type": "keyword"},
                        "PA_CMP": {"type": "keyword"},
                        "PA_PROC_ID": {"type": "integer"},
                        "PA_TPFIN": {"type": "keyword"},
                        "PA_SUBFIN": {"type": "integer"},
                        "PA_AUTORIZ": {"type": "keyword"},
                        "PA_CIDPRI": {"type": "keyword"},
                        "PA_CIDSEC": {"type": "keyword"},
                        "PA_CATEND": {"type": "keyword"},
                        "PA_QTDPRO": {"type": "integer"},
                        "PA_QTDAPR": {"type": "integer"},
                        "PA_VALPRO": {"type": "keyword"},
                        "PA_VALAPR": {"type": "keyword"},
                    }
                }
            })

        actions = (prepare_es_doc(record, index_name)
                   for record in data_chunk if record)

        for attempt in range(MAX_RETRIES):
            try:
                # Ensure chunk_size is passed as a keyword argument
                for ok, _ in helpers.streaming_bulk(es, actions, chunk_size=INT_CHUNK_SIZE):
                    if not ok:
                        logging.error("A document failed to index.")
                break
            except Exception as e:
                logging.error(
                    f"Error during bulk indexing to {index_name} (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)}")
                time.sleep(RETRY_DELAY)
        else:
            logging.error(
                f"Failed to index chunk after {MAX_RETRIES} attempts.")
    except Exception as e:
        logging.error(
            f"Error processing chunk for index {index_name}: {str(e)}")


def parallel_bulk_index(dbf_directory):
    start_time = time.time()
    logging.info("Starting parallel indexing process.")

    dbf_files = [f for f in os.listdir(dbf_directory) if f.endswith('.dbf')]
    tasks = []

    with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        for dbf_file in dbf_files:
            index_name = f"{ES_INDEX_NAME_PREFIX}{os.path.splitext(dbf_file)[0]}".lower(
            ).replace(" ", "_").replace("-", "_")
            dbf_file_path = os.path.join(dbf_directory, dbf_file)
            try:
                logging.info(f"Reading DBF file: {dbf_file}")
                for data_chunk in read_in_batches(dbf_file_path, INT_CHUNK_SIZE):
                    tasks.append(executor.submit(
                        process_chunk, data_chunk, index_name))
            except Exception as e:
                logging.error(f"Failed to read DBF file {dbf_file}: {str(e)}")

        for future in as_completed(tasks):
            try:
                future.result()  # Will raise an exception if one occurred in `process_chunk`
            except Exception as exc:
                logging.error(f"An error occurred: {exc}")

    elapsed_time = time.time() - start_time
    logging.info(
        f"Data extraction and indexing took: {elapsed_time:.2f} seconds.")


# Start the parallel indexing process
if __name__ == "__main__":
    try:
        parallel_bulk_index(dbf_directory)
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
