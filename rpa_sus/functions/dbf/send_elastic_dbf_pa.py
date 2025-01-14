from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
import datetime
from dbfread import DBF
from elasticsearch import Elasticsearch, helpers
import logging
import os
import time

from configs.database import ELASTICSEARCH_HOST, ES_INDEX_NAME_PREFIX, CHUNK_SIZE, MAX_RETRIES, RETRY_DELAY, NUM_PROCESSES, ELASTIC_USERNAME, ELASTIC_PASSWORD
from configs.constants import CARATEND_CODES, COLUMNS_TO_WATCH, state_codes, FINANC_CODES, FAECTP_CODES


# DBF directory path
dbf_directory = './data/pa_acre'  # Specify the directory containing DBF files

# dbf_directory = '/mnt/volume_nyc1_01/nicolas/alagoas/PA_ACIMA_2008'

# Create Elasticsearch client with comprehensive error handling
try:
    es_client = Elasticsearch(
        [ELASTICSEARCH_HOST],
        basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
    ).options(request_timeout=60)
except Exception as connection_error:
    print(f"Elasticsearch connection failed: {connection_error}")


INT_CHUNK_SIZE = int(CHUNK_SIZE)

# Logging setup
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


# Precomputed mapping for state codes
code_to_state = {value: key for key, value in state_codes.items()}

# # Precomputed mapping for financial codes
financ_codes_to_name = {value: key for key, value in FINANC_CODES.items()}


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


def clean_column_data(record):
    """Clean and preprocess the columns in the record before manipulation."""
    cleaned_record = {}

    for key, value in record.items():
        if value is None:
            # Or assign a default value like "" or 0 if preferred
            cleaned_record[key] = None
            continue

        # Ensure all values are strings, trimming whitespaces
        if isinstance(value, str):
            cleaned_value = value.strip()  # Remove leading/trailing whitespaces
        else:
            cleaned_value = value

        # Add further transformations if needed, for example:
        # - Clean date format, currency formatting, etc.
        # - Normalize text data, e.g., make all text lowercase if required
        # - Remove unwanted characters or standardize codes

        cleaned_record[key] = cleaned_value

    return cleaned_record


def prepare_es_doc(record, index_name, fields_to_include=COLUMNS_TO_WATCH):
    """Prepare a document for Elasticsearch by cleaning and transforming data."""
    # Step 1: Clean the record by preprocessing columns
    cleaned_record = clean_column_data(record)

    # Step 2: Filter the record to include only specified fields (optional)
    if fields_to_include:
        cleaned_record = {
            k: v for k, v in cleaned_record.items() if k in fields_to_include}

    # Step 3: Handle PA_UFMUN conversion
    uf_code = str(cleaned_record.get("PA_UFMUN", ""))[:2]
    cleaned_record["PA_UFMUN"] = get_mapped_value(uf_code, code_to_state)

    # # Step 4: Handle PA_CMP conversion to yyyyMM format
    # pa_cmp = cleaned_record.get("PA_CMP", "")
    # if pa_cmp:
    #     cleaned_record["PA_CMP"] = handle_date_conversion(pa_cmp)
    #     if cleaned_record["PA_CMP"]:
    #         cleaned_record["@timestamp"] = cleaned_record["PA_CMP"]

    # # Step 5: Handle PA_TPFIN conversion to financial code
    # pa_tpfin = cleaned_record.get("PA_TPFIN", "")
    # if pa_tpfin:
    #     cleaned_record["PA_TPFIN"] = get_mapped_value(
    #         pa_tpfin, financ_codes_to_name)

    # # Step 6: Handle PA_SUBFIN conversion to faectp code
    # pa_subfin = cleaned_record.get("PA_SUBFIN", "")
    # if pa_subfin:
    #     pa_subfin = pa_subfin.lstrip('0')
    #     cleaned_record["PA_SUBFIN"] = get_mapped_value(pa_subfin, FAECTP_CODES)

    # # Step 7: Handle PA_CARATEND conversion to caratend code
    # pa_caratend = cleaned_record.get("PA_CATEND", "")
    # if pa_caratend:
    #     cleaned_record["PA_CATEND"] = get_mapped_value(
    #         pa_caratend, CARATEND_CODES)

    # Step 8: Prepare the final document for Elasticsearch
    return {
        '_index': index_name,
        '_source': {k: str(v) if v is not None else None for k, v in cleaned_record.items()},
    }


def ensure_index_exists(index_name: str):
    # Check if the index exists and create it if not
    if not es_client.indices.exists(index=index_name):
        es_client.indices.create(
            index=index_name,
            body={
                "mappings": {
                    "properties": {
                        "PA_CODUNI": {"type": "keyword"},
                        "PA_UFMUN": {"type": "keyword"},
                        "PA_CNPJCPF": {"type": "keyword"},
                        "PA_CNPJMNT": {"type": "keyword"},
                        "PA_CMP": {"type": "keyword"},
                        "PA_PROC_ID": {"type": "integer"},
                        "PA_TPFIN": {"type": "keyword"},
                        "PA_SUBFIN": {"type": "keyword"},
                        "PA_AUTORIZ": {"type": "keyword"},
                        "PA_CIDPRI": {"type": "keyword"},
                        "PA_CIDSEC": {"type": "keyword"},
                        "PA_CATEND": {"type": "keyword"},
                        "PA_QTDPRO": {"type": "integer"},
                        "PA_QTDAPR": {"type": "integer"},
                        "PA_VALPRO": {"type": "float"},
                        "PA_VALAPR": {"type": "float"},
                    }
                }
            }
        )


def read_in_batches(dbf_file_path, batch_size):
    """Read the DBF file in batches and count the records."""
    record_count = 0  # Initialize a counter for the records processed
    try:
        # Open the DBF file and pass the file path to DBF
        reader = DBF(dbf_file_path)  # Pass the file path directly to DBF
        data_batch = []
        for record in reader:
            data_batch.append(record)
            record_count += 1  # Increment the counter for each record processed
            if len(data_batch) == batch_size:
                yield data_batch
                data_batch = []

        # If there are any remaining records in the final batch, yield them
        if data_batch:
            yield data_batch

        # Log the total number of records processed
        logging.info(
            f"Total records processed from {dbf_file_path}: {record_count}")

    except Exception as e:
        logging.error(
            f"ERROR - Failed to read DBF file {dbf_file_path}: {str(e)}")
        raise


# Initialize global counter to track processed records
processed_records_count = 0


def process_chunk(data_chunk, index_name):
    """Process a chunk of data and send it to Elasticsearch."""
    try:
        actions = (
            prepare_es_doc(record, index_name)
            for record in data_chunk if record
        )

        record_count = 0  # Counter to track the number of records processed
        failed_documents = 0  # Track the number of failed documents

        # Attempting bulk indexing with retries
        for attempt in range(MAX_RETRIES):
            try:
                # Perform bulk indexing and check if documents are indexed successfully
                for ok, response in helpers.streaming_bulk(
                    client=es_client, actions=actions, chunk_size=INT_CHUNK_SIZE
                ):
                    if not ok:
                        failed_documents += 1
                        # Log the error but avoid printing entire response, log only the error reason
                        error_message = response.get('error', {}).get(
                            'reason', 'Unknown error')
                        logging.error(
                            f"Failed to index document: {error_message}")
                    else:
                        record_count += 1  # Increment counter for each successful record

                # If no documents failed, exit the loop
                if failed_documents == 0:
                    break
            except Exception as e:
                # Log the error and retry if necessary
                logging.error(
                    f"Error during bulk indexing to {index_name} (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)}")
                # Exponential backoff for retry
                time.sleep(RETRY_DELAY * (2 ** attempt))
        else:
            # If all attempts fail, log the failure
            logging.error(
                f"Failed to index chunk after {MAX_RETRIES} attempts.")

        if failed_documents > 0:
            logging.error(
                f"{failed_documents} document(s) failed to index in total.")

        return record_count  # Return the count of processed records in this chunk

    except Exception as e:
        # Catching and logging any exceptions during the chunk processing
        logging.error(
            f"Error processing chunk for index {index_name}: {str(e)}")
        return 0  # If an error occurs, return 0 to ensure no records are counted


def parallel_bulk_index(dbf_directory):
    start_time = time.time()
    logging.info("Starting parallel indexing process.")

    dbf_files = [f for f in os.listdir(dbf_directory) if f.endswith('.dbf')]

    # Create a list to keep track of futures
    futures = []

    # Use ProcessPoolExecutor for parallel processing
    with ProcessPoolExecutor(max_workers=NUM_PROCESSES) as executor:
        for dbf_file in dbf_files:
            index_name = f"{ES_INDEX_NAME_PREFIX}{os.path.splitext(dbf_file)[0]}".lower(
            ).replace(" ", "_").replace("-", "_")
            dbf_file_path = os.path.join(dbf_directory, dbf_file)

            # Create the index if it doesn't exist
            ensure_index_exists(index_name)
            try:
                logging.info(f"Reading DBF file: {dbf_file}")
                # Submit tasks for each chunk and store the futures
                for data_chunk in read_in_batches(dbf_file_path, INT_CHUNK_SIZE):
                    future = executor.submit(
                        process_chunk, data_chunk, index_name)
                    futures.append(future)  # Append the future to the list
            except Exception as e:
                logging.error(f"Failed to read DBF file {dbf_file}: {str(e)}")

    # Wait for all futures to complete and handle any potential exceptions
    for future in as_completed(futures):
        try:
            future.result()  # Will raise an exception if one occurred during processing
        except Exception as exc:
            logging.error(f"An error occurred during chunk processing: {exc}")

    elapsed_time = time.time() - start_time
    elapsed_minutes = elapsed_time // 60
    elapsed_seconds = elapsed_time % 60
    logging.info(
        f"Data extraction and indexing took: {int(elapsed_minutes)} minutes and {elapsed_seconds:.2f} seconds.")


# Start the parallel indexing process
if __name__ == "__main__":
    try:
        parallel_bulk_index(dbf_directory)
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
