# fmt: off
# Add the project root directory to Python path
import sys
import os
project_root = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..', '..', '..'))
sys.path.insert(0, project_root)

import logging
import time
from urllib3.exceptions import InsecureRequestWarning
import warnings
from tenacity import retry, stop_after_attempt, wait_exponential
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from elasticsearch import Elasticsearch, helpers, exceptions
import traceback
import http.client
import gc
import psutil

from rpa_sus.functions.dbf.common.clean_data_record import clean_column_data
from rpa_sus.functions.dbf.common.read_in_batches import read_in_batches
from rpa_sus.configs.constants import state_codes
from rpa_sus.configs.database import CHUNK_SIZE, NUM_PROCESSES, ELASTICSEARCH_HOST, ES_INDEX_NAME_PREFIX, MAX_RETRIES, RETRY_DELAY, ELASTIC_USERNAME, ELASTIC_PASSWORD

http.client._MAXLINE = 1000000  # Increase the maximum line length

#fmton

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s,%(msecs)03d - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# Suppress specific Elasticsearch transport warnings
logging.getLogger("elastic_transport.node_pool").setLevel(logging.ERROR)

# If you're using urllib3 with verify=False, also suppress these warnings
warnings.simplefilter('ignore', InsecureRequestWarning)

dbf_directory = '/mnt/volume_nyc1_01/nicolas/rd-98-03'

INT_CHUNK_SIZE = int(CHUNK_SIZE)

COLUNS_TO_WATCH_98_03 = [
    "UF_ZI",
    "ANO_CMPT",
    "MES_CMPT",
    "ESPEC",
    "CGC_HOSP",
    "N_AIH",
    "IDENT",
    "UTI_MES_TO",
    "PROC_REA",
    "VAL_SH",
    "VAL_SP",
    "VAL_SADT",
    "VAL_TOT",
    "VAL_UTI",
    "DT_INTER",
    "DT_SAIDA",
    "DIAG_PRINC",
    "COBRANCA",
    "NATUREZA",
    "GESTAO",
    "MUNIC_MOV",
    "DIAS_PERM",
]


# Initialize global counter to track processed records
processed_records_count = 0

MAX_BACKOFF = 60  # Maximum backoff time in seconds

# Create Elasticsearch client with comprehensive error handling
try:
    es_client = Elasticsearch(
        [ELASTICSEARCH_HOST],
        basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
        retry_on_timeout=True,
        max_retries=MAX_RETRIES,
    ).options(request_timeout=120)  # Increase request timeout to 120 seconds

    # Check if the connection is successful
    if es_client.ping():
        logging.info("Connected to Elasticsearch!")

except Exception as connection_error:
    logging.error(f"Elasticsearch connection failed: {connection_error}")
    logging.error(f"Traceback: {traceback.format_exc()}")
    sys.exit(1)


INT_CHUNK_SIZE = int(5000)

# Precomputed mapping for state codes
code_to_state = {value: key for key, value in state_codes.items()}


# Function to generate a unique document ID based on multiple fields
def generate_document_id(record):
    """Generate a unique document ID based on multiple fields."""
    # Select fields that together make a record unique
    # Adjust these fields based on your data structure
    unique_fields = [
        record.get("UF_ZI", ""),
        record.get("ANO_CMPT", ""),
        record.get("MES_CMPT", ""),
        record.get("CGC_HOSP", ""),
        record.get("N_AIH", "")
    ]
    
    # Join the fields with a separator and create a hash
    unique_id = "_".join(str(field) for field in unique_fields if field)
    
    # Return the unique ID
    return unique_id

def get_mapped_value(code, mapping, default="Unknown"):
    """Helper function to get mapped value from a dictionary."""
    try:
        return mapping.get(int(code), default)
    except ValueError:
        return default


def handle_date_conversion(date_value):
    """Helper function to convert date to yyyyMM format."""
    try:
        # Assuming date_value is in the format YYYYMM or a valid integer
        return datetime.datetime.strptime(str(date_value), "%Y%m").strftime("%Y%m")
    except ValueError:
        return None


def handle_data_conversion_rd_97_03(ANO, MES):
    """Handle data conversion for PA_CMP files."""
    # Step 5: Handle PA_CMP conversion to datetime format
    try:
        ano_cmpt = int(ANO)
        mes_cmpt = int(MES)

        # Ensure the month is valid (1-12)
        if 1 <= mes_cmpt <= 12:
            # Create a datetime object for the first day of the given year and month
            date_value = datetime(ano_cmpt, mes_cmpt, 1)
            # Format the date as ISO 8601 format which Elasticsearch can parse
            formatted_date = date_value.strftime("%Y-%m-%dT00:00:00")
            return formatted_date
        else:
            return None
    except ValueError:
        # If conversion fails, return None
        return None


def prepare_es_doc(record, index_name, fields_to_include=COLUNS_TO_WATCH_98_03):
    """Prepare a document for Elasticsearch by cleaning and transforming data."""
    # Step 1: Clean the record by preprocessing columns
    cleaned_record = clean_column_data(record)

    # Step 3: Handle UF_ZI conversion
    cleaned_record["UF_ZI"] = get_mapped_value(
        cleaned_record["UF_ZI"], code_to_state)

    # Step 2: Filter the record to include only specified fields (optional)
    if fields_to_include:
        cleaned_record = {
            k: v for k, v in cleaned_record.items() if k in fields_to_include}

    # # Step 4: Handle PA_CMP conversion to yyyyMM format
    ano_cmpt = cleaned_record.get("ANO_CMPT", "")
    mes_cmpt = cleaned_record.get("MES_CMPT", "")
    if ano_cmpt and mes_cmpt:
        date_value = handle_data_conversion_rd_97_03(ano_cmpt, mes_cmpt)
        if date_value:
            # Use the datetime value
            formatted_date = date_value  # Format as needed
        else:
            # Handle the case where conversion wasn't possible
            formatted_date = None
        cleaned_record["@DATA"] = formatted_date  # Update the field

    # Generate a unique document ID
    doc_id = generate_document_id(cleaned_record)

    # Step 8: Prepare the final document for Elasticsearch
    return {
        '_index': index_name,
        '_id': doc_id,  # Add the unique document ID
        '_source': {k: str(v) if v is not None else None for k, v in cleaned_record.items()},
    }


def ensure_index_exists(index_name: str):
    try:
        # Check if the index exists
        if not es_client.indices.exists(index=index_name):
            # If it doesn't exist, try to create it
            es_client.indices.create(
                index=index_name,
                body={
                    "settings": {
                        "number_of_shards": 3,  # Adjust based on your cluster size
                        "number_of_replicas": 1,  # Adjust based on your needs
                        "refresh_interval": "30s",  # Reduce refresh frequency during bulk indexing
                        "translog": {
                            "durability": "async",  # Async translog for better performance
                            "sync_interval": "30s"
                        }
                    },
                    "mappings": {
                        "properties": {
                            "UF_ZI": {"type": "keyword"},
                            "ANO_CMPT": {"type": "keyword"},
                            "MES_CMPT": {"type": "keyword"},
                            "ESPEC": {"type": "keyword"},
                            "CGC_HOSP": {"type": "keyword"},
                            "N_AIH": {"type": "keyword"},
                            "IDENT": {"type": "keyword"},
                            "UTI_MES_TO": {"type": "integer"},
                            "PROC_REA": {"type": "keyword"},
                            "VAL_SH": {"type": "float"},
                            "VAL_SP": {"type": "float"},
                            "VAL_SADT": {"type": "float"},
                            "VAL_TOT": {"type": "float"},
                            "VAL_UTI": {"type": "float"}, #98 -> 2003
                            "DT_INTER": {"type": "keyword"},
                            "DT_SAIDA": {"type": "keyword"},
                            "DIAG_PRINC": {"type": "keyword"},
                            "COBRANCA": {"type": "keyword"},
                            "NATUREZA": {"type": "keyword"},
                            "GESTAO": {"type": "keyword"}, #98 -> 2003
                            "MUNIC_MOV": {"type": "keyword"},
                            "DIAS_PERM": {"type": "keyword"},
                            "@DATA": {"type": "date"},
                            "VAL_GERAL": {"type": "float"},
                        }
                    }
                }
            )
            logging.info(f"Index '{index_name}' created successfully.")
        else:
            logging.info(f"Index '{index_name}' already exists.")
    except exceptions.ConnectionError as ce:
        logging.error(f"Connection error while ensuring index exists: {ce}")
        raise
    except exceptions.RequestError as re:
        logging.error(f"Request error while ensuring index exists: {re}")
        logging.error(f"Error info: {re.info}")
        logging.error(f"Error status: {re.status_code}")
        if re.error == 'resource_already_exists_exception':
            logging.info(f"Index '{index_name}' already exists.")
        else:
            raise
    except exceptions.AuthenticationException as ae:
        logging.error(f"Authentication error: {ae}")
        raise
    except exceptions.AuthorizationException as ae:
        logging.error(f"Authorization error: {ae}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error while ensuring index exists: {e}")
        raise


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
def process_chunk_with_retry(data_chunk, index_name, chunk_index, dbf_file):
    return process_chunk(data_chunk, index_name, chunk_index, dbf_file)


def process_chunk(data_chunk, index_name, chunk_index, dbf_file):
    """Process a chunk of data and send it to Elasticsearch."""
    try:
        # Filter out None records
        valid_records = [record for record in data_chunk if record]

        if not valid_records:
            logging.info(
                f"Chunk {chunk_index} for file '{dbf_file}' has no valid records, skipping")
            return 0

        # Create actions with unique document IDs
        actions = list(prepare_es_doc(record, index_name)
                       for record in valid_records)
        chunk_size = len(actions)

        logging.info(
            f"Processing chunk {chunk_index} for file '{dbf_file}' (Index: '{index_name}', Size: {chunk_size})")

        success_count = 0
        failed_documents = 0

        # Use a smaller chunk size for streaming_bulk to avoid memory issues
        streaming_chunk_size = min(2500, INT_CHUNK_SIZE)

        start_time = time.time()
        
        # Use helpers.bulk instead of streaming_bulk for better handling of document IDs
        success, failed = helpers.bulk(
            client=es_client,
            actions=actions,
            chunk_size=streaming_chunk_size,
            max_retries=MAX_RETRIES,
            stats_only=True  # Return only success/failed counts
        )
        
        success_count = success
        failed_documents = failed

        duration = time.time() - start_time
        logging.info(
            f"Chunk {chunk_index} complete: {success_count} indexed, {failed_documents} failed in {duration:.2f}s")

        return success_count

    except Exception as e:
        logging.error(
            f"Unexpected error processing chunk {chunk_index} for file '{dbf_file}' (Index: '{index_name}'): {str(e)}",
            exc_info=True
        )
        return 0
        return 0


def parallel_bulk_index(dbf_directory):
    start_time = time.time()
    logging.info("Starting parallel indexing process.")

    total_processed = 0
    total_files = 0
    total_chunks = 0
    failed_files = 0

    # Get all DBF files and sort them for predictable processing order
    try:
        dbf_files = sorted([f for f in os.listdir(
            dbf_directory) if f.endswith('.dbf')])
        total_file_count = len(dbf_files)
        logging.info(f"Found {total_file_count} DBF files to process")
    except Exception as e:
        logging.error(
            f"Failed to list DBF files in directory {dbf_directory}: {str(e)}")
        return

    # Process files in batches to control memory usage
    # Reduce batch size relative to worker count
    batch_size = min(total_file_count, NUM_PROCESSES * 2)

    # Calculate optimal worker count based on available system resources
    total_memory_gb = psutil.virtual_memory().total / (1024 * 1024 * 1024)
    cpu_count = os.cpu_count() or 1

    # Allocate ~1GB per worker, but cap at CPU count or 8, whichever is lower
    optimal_workers = min(int(total_memory_gb / 1.5), cpu_count, NUM_PROCESSES)

    # Use at least 1 worker, but no more than 8
    worker_count = max(1, min(optimal_workers, NUM_PROCESSES))

    logging.info(
        f"System has {cpu_count} CPUs and {total_memory_gb:.1f}GB RAM")
    logging.info(f"Using {worker_count} worker processes")

    # Create a separate Elasticsearch client for each process
    es_clients = {}

    for batch_start in range(0, total_file_count, batch_size):
        batch_end = min(batch_start + batch_size, total_file_count)
        batch_files = dbf_files[batch_start:batch_end]

        logging.info(
            f"Processing batch of {len(batch_files)} files ({batch_start+1}-{batch_end} of {total_file_count})")

        # Process one file at a time to avoid race conditions
        for dbf_file in batch_files:
            index_name = f"{ES_INDEX_NAME_PREFIX}{os.path.splitext(dbf_file)[0]}".lower(
            ).replace(" ", "_").replace("-", "_")
            dbf_file_path = os.path.join(dbf_directory, dbf_file)

            # Create the index if it doesn't exist
            ensure_index_exists(index_name)
            
            try:
                logging.info(
                    f"Reading DBF file: {dbf_file} ({total_files+1}/{total_file_count})")
                total_files += 1

                # Create a list to keep track of futures for this file
                futures = []
                
                # Use ProcessPoolExecutor with our calculated optimal worker count
                with ProcessPoolExecutor(max_workers=worker_count) as executor:
                    # Submit tasks for each chunk and store the futures
                    file_chunks = 0
                    for chunk_index, data_chunk in enumerate(read_in_batches(dbf_file_path, INT_CHUNK_SIZE), 1):
                        total_chunks += 1
                        file_chunks += 1
                        future = executor.submit(
                            process_chunk_with_retry, data_chunk, index_name, chunk_index, dbf_file)
                        futures.append(future)

                    logging.info(
                        f"Submitted {file_chunks} chunks for processing from file {dbf_file}")
                    
                    # Wait for all futures for this file to complete before moving to the next file
                    file_processed = 0
                    for future in as_completed(futures):
                        try:
                            processed_count = future.result()
                            file_processed += processed_count
                            total_processed += processed_count
                        except Exception as exc:
                            logging.error(
                                f"An error occurred during chunk processing: {exc}")
                    
                    logging.info(f"Completed processing file {dbf_file}: {file_processed} records indexed")

            except Exception as e:
                failed_files += 1
                logging.error(
                    f"Failed to read DBF file {dbf_file}")

        # Clear memory after processing the batch
        clear_memory()

        # Log progress after each batch
        current_elapsed = time.time() - start_time
        current_minutes = int(current_elapsed // 60)
        current_seconds = current_elapsed % 60
        logging.info(
            f"Batch complete: {batch_end}/{total_file_count} files processed")
        logging.info(
            f"Current progress: {total_processed} records indexed in {current_minutes}m {current_seconds:.2f}s")
        logging.info(f"Memory cleared after batch processing")
        logging.info("\n")

    elapsed_time = time.time() - start_time
    elapsed_minutes = int(elapsed_time // 60)
    elapsed_seconds = elapsed_time % 60

    logging.info("=" * 50)
    logging.info("Indexing Process Summary:")
    logging.info(f"Total files processed: {total_files}")
    logging.info(f"Failed files: {failed_files}")
    logging.info(f"Total chunks processed: {total_chunks}")
    logging.info(f"Total records indexed: {total_processed}")
    logging.info(
        f"Total time: {elapsed_minutes} minutes and {elapsed_seconds:.2f} seconds")
    logging.info(
        f"Average indexing rate: {total_processed/elapsed_time:.2f} records/second")
    logging.info("\n")
    logging.info("Data processing has finished.")


def clear_memory():

    # Force garbage collection
    gc.collect()

    # Log memory usage
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    logging.info(
        f"Memory usage after GC: {memory_info.rss / 1024 / 1024:.2f} MB")

# Start the parallel indexing process
if __name__ == "__main__":
    try:
        parallel_bulk_index(dbf_directory)
        logging.info("Data processing has completed successfully.")
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user. Finishing up...")
    except Exception as e:
        logging.error(f"Unexpected error EXECPTION ERROR RONALDO",)
    finally:
        logging.info("Script execution has ended.")
        # Optionally, you can add a cleanup function here if needed
