# fmt: off
import sys
import os
import traceback

# Add the project root directory to Python path
project_root = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..', '..', '..'))
sys.path.insert(0, project_root)

import time
import logging
from elasticsearch import Elasticsearch, helpers, exceptions
from http.client import LineTooLong
import datetime
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

from rpa_sus.configs.database import ELASTICSEARCH_HOST, ES_INDEX_NAME_PREFIX, MAX_RETRIES, RETRY_DELAY, NUM_PROCESSES, ELASTIC_USERNAME, ELASTIC_PASSWORD
from rpa_sus.configs.constants import state_codes
from rpa_sus.functions.dbf.common import read_in_batches
from rpa_sus.functions.dbf.common.clean_data_record import clean_column_data
# fmt: on


COLUNS_TO_WATCH_92_97 = [
    "UF_ZI",
    "ANO_CMPT",
    "MES_CMPT",
    "ESPEC",
    "CGC_HOSP",
    "N_AIH",
    "IDENT",
    "UTI_TOTAL",
    "PROC_REA",
    "VAL_SH",
    "VAL_SP",
    "VAL_SADT",
    "VAL_TOT",
    "DT_INTER",
    "DT_SAIDA",
    "DIAG_PRINC",
    "COBRANCA",
    "NATUREZA",
    "MUNIC_MOV",
    "DIAS_PERM",
]

INT_CHUNK_SIZE = 3000  # Adjust the chunk size for bulk indexing

# dbf_directory = './data/pa_acre'  # Specify the directory containing DBF files


dbf_directory = '/home/nicolas/FreeLancers/FlavioProject/rpa_sus/data'

# Create Elasticsearch client with comprehensive error handling
try:
    es_client = Elasticsearch(
        ["http://localhost:9200"],
        basic_auth=('elastic', 'NICOLAS123@@@'),
        api_key=None,
        verify_certs=False,
    ).options(request_timeout=60)

    # Check if the connection is successful
    indices_status = es_client.cat.indices(format="json")
    print("Indices Status:", indices_status)

except Exception as connection_error:
    logging.error(f"Elasticsearch connection failed: {connection_error}")
    logging.error(f"Traceback: {traceback.format_exc()}")
    sys.exit(1)


INT_CHUNK_SIZE = int(INT_CHUNK_SIZE)

# Logging setup
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


# Precomputed mapping for state codes
code_to_state = {value: key for key, value in state_codes.items()}


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


def handle_data_conversion_92_97(ANO, MES):
    """Handle data conversion for PA_CMP files."""
    # Step 5: Handle PA_CMP conversion to datetime format
    try:
        ano_cmpt = int(ANO)
        mes_cmpt = int(MES)

        # Ensure the month is valid (1-12)
        if 1 <= mes_cmpt <= 12:
            # Create a datetime object for the first day of the given year and month
            date_value = datetime(ano_cmpt, mes_cmpt, 1)
            return date_value
        else:
            return None
    except ValueError:
        # If conversion fails, return None
        return None


def prepare_es_doc(record, index_name, fields_to_include=COLUNS_TO_WATCH_92_97):
    """Prepare a document for Elasticsearch by cleaning and transforming data."""
    # Step 1: Clean the record by preprocessing columns
    cleaned_record = clean_column_data(record)

    # # Step 3: Handle UF_ZI conversion
    # cleaned_record["UF_ZI"] = get_mapped_value(
    #     cleaned_record["UF_ZI"], code_to_state)

    # Step 2: Filter the record to include only specified fields (optional)
    # if fields_to_include:
    #     cleaned_record = {
    #         k: v for k, v in cleaned_record.items() if k in fields_to_include}

    # # # Step 4: Handle PA_CMP conversion to yyyyMM format
    # ano_cmpt = cleaned_record.get("ANO_CMPT", "")
    # mes_cmpt = cleaned_record.get("MES_CMPT", "")
    # if ano_cmpt and mes_cmpt:
    #     date_value = handle_data_conversion_92_97(ano_cmpt, mes_cmpt)
    #     if date_value:
    #         # Use the datetime value
    #         formatted_date = date_value.strftime(
    #             "%Y-%m-%d")  # Format as needed
    #     else:
    #         # Handle the case where conversion wasn't possible
    #         formatted_date = None
    #     cleaned_record["@DATA"] = formatted_date  # Update the field

    # Step 8: Prepare the final document for Elasticsearch
    return {
        '_index': index_name,
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
                    "mappings": {
                        "properties": {
                            "UF_ZI": {"type": "keyword"},
                            "ANO_CMPT": {"type": "keyword"},
                            "MES_CMPT": {"type": "keyword"},
                            "ESPEC": {"type": "keyword"},
                            "CGC_HOSP": {"type": "keyword"},
                            "N_AIH": {"type": "keyword"},
                            "IDENT": {"type": "keyword"},
                            "UTI_TOTAL": {"type": "integer"},
                            "PROC_REA": {"type": "keyword"},
                            "VAL_SH": {"type": "float"},
                            "VAL_SP": {"type": "float"},
                            "VAL_SADT": {"type": "float"},
                            "VAL_TOT": {"type": "float"},
                            "DT_INTER": {"type": "keyword"},
                            "DT_SAIDA": {"type": "keyword"},
                            "DIAG_PRINC": {"type": "keyword"},
                            "COBRANCA": {"type": "keyword"},
                            "NATUREZA": {"type": "keyword"},
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


# Initialize global counter to track processed records
processed_records_count = 0


MAX_BACKOFF = 60  # Maximum backoff time in seconds


def process_chunk(data_chunk, index_name):
    """Process a chunk of data and send it to Elasticsearch."""
    try:
        actions = (
            prepare_es_doc(record, index_name)
            for record in data_chunk if record
        )

        record_count = 0
        failed_documents = 0

        for attempt in range(MAX_RETRIES):
            try:
                for ok, response in helpers.streaming_bulk(
                    client=es_client, actions=actions, chunk_size=INT_CHUNK_SIZE, max_retries=3
                ):
                    if not ok:
                        failed_documents += 1
                        error_message = response.get('error', {}).get(
                            'reason', 'Unknown error')
                        logging.error(
                            f"Failed to index document: {error_message}")
                    else:
                        record_count += 1

                if failed_documents == 0:
                    break
            except LineTooLong as e:
                logging.error(
                    f"LineTooLong error: {e}. Try reducing chunk size.")
                break
            except Exception as e:
                backoff = min(RETRY_DELAY * (2 ** attempt), MAX_BACKOFF)
                logging.error(
                    f"Error during bulk indexing (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                time.sleep(backoff)
        else:
            logging.error(
                f"Failed to index chunk after {MAX_RETRIES} attempts.")

        if failed_documents > 0:
            logging.error(f"{failed_documents} document(s) failed to index.")

        return record_count

    except Exception as e:
        logging.error(f"Error processing chunk for index {index_name}: {e}")
        return 0


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
        logging.error(f"Unexpected error: {str(e)}", exc_info=True)
