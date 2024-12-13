import base64
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
import datetime
import json
from dbfread import DBF
from elastic_transport import NodeConfig, Transport
from elasticsearch import Elasticsearch, helpers
import logging
import os
import time

from configs.database import ELASTICSEARCH_HOST, ES_INDEX_NAME_PREFIX, CHUNK_SIZE, MAX_RETRIES, RETRY_DELAY, NUM_PROCESSES, ELASTIC_USERNAME, ELASTIC_PASSWORD
from configs.constants import CARATEND_CODES, COLUMNS_TO_WATCH, state_codes, FINANC_CODES, FAECTP_CODES


# DBF directory path
dbf_directory = './data/pa_acre'  # Specify the directory containing DBF files

# dbf_directory = '/mnt/volume_nyc1_01/nicolas/alagoas/PA_ACIMA_2008'


def debug_sniff_callback(transport, options):
    """Enhanced debugging for sniff callback with explicit authentication"""
    try:
        print("Attempting to sniff nodes...")

        # Use the same authentication method as the main connection
        response = transport.perform_request(
            "GET",
            "/_nodes/http",
            # Explicitly pass authentication
            headers={
                "Authorization": f"Basic {base64.b64encode(f'{ELASTIC_USERNAME}:{ELASTIC_PASSWORD}'.encode()).decode()}"
            }
        )

        print("Raw Response:", response)

        # Ensure response has body attribute or is a dictionary
        body = response.body if hasattr(response, 'body') else response

        if "nodes" not in body:
            print("Warning: 'nodes' key not found in response")
            return []

        sniffed_nodes = []
        for node_id, node_info in body["nodes"].items():
            try:
                # More robust parsing of publish address
                publish_address = node_info["http"]["publish_address"]
                # Handle different possible formats of publish address
                if ':' in publish_address:
                    host, port = publish_address.split(":")
                else:
                    host, port = publish_address, "9200"

                node = NodeConfig(
                    host=host.strip('[]'),  # Remove square brackets for IPv6
                    port=int(port),
                    scheme="http" if not node_info.get(
                        "https", {}).get("enabled") else "https"
                )
                sniffed_nodes.append(node)
                print(f"Discovered node: {node}")
            except Exception as node_error:
                print(f"Error processing node {node_id}: {node_error}")

        return sniffed_nodes

    except Exception as e:
        print(f"Sniff callback failed: {e}")
        return []


# Node configurations with error handling
node_configs = [
    NodeConfig(
        host="localhost",
        port=9200,
        scheme="http",
    )
]

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


def prepare_es_doc(record, index_name, fields_to_include=COLUMNS_TO_WATCH):
    # Filter record to include only specified fields
    if fields_to_include:
        record = {k: v for k, v in record.items() if k in fields_to_include}

    # Handle PA_UFMUN conversion
    uf_code = str(record.get("PA_UFMUN", ""))[:2]
    record["PA_UFMUN"] = get_mapped_value(uf_code, code_to_state)

    # # Handle PA_CMP conversion to yyyyMM format
    # pa_cmp = record.get("PA_CMP", "")
    # if pa_cmp:
    #     record["PA_CMP"] = handle_date_conversion(pa_cmp)
    #     if record["PA_CMP"]:
    #         record["@timestamp"] = record["PA_CMP"]

    # # Handle PA_TPFIN conversion to financial code
    # pa_tpfin = record.get("PA_TPFIN", "")
    # if pa_tpfin:
    #     record["PA_TPFIN"] = get_mapped_value(pa_tpfin, financ_codes_to_name)

    # # Handle PA_SUBFIN conversion to faectp code (with validation for integer type)
    # pa_subfin = record.get("PA_SUBFIN", "")
    # if pa_subfin:
    #     pa_subfin = pa_subfin.lstrip('0')
    #     record["PA_SUBFIN"] = get_mapped_value(pa_subfin, FAECTP_CODES)

    # # Handle PA_CARATEND conversion to caratend code
    # pa_caratend = record.get("PA_CATEND", "")
    # if pa_caratend:
    #     record["PA_CATEND"] = get_mapped_value(
    #         pa_caratend, CARATEND_CODES)

    # Prepare the final document
    return {
        '_index': index_name,
        '_source': {k: str(v) if v is not None else None for k, v in record.items()},
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


def log_bulk_size(actions):
    size_in_bytes = sum(len(json.dumps(action)) for action in actions)
    print(f"Bulk request size: {size_in_bytes} bytes")

# # Function to process a chunk of data and send it to Elasticsearch


def process_chunk(data_chunk, index_name):
    """Process a chunk of data and send it to Elasticsearch."""
    try:
        actions = (
            prepare_es_doc(record, index_name)
            for record in data_chunk if record
        )

        # Attempting bulk indexing with retries
        for attempt in range(MAX_RETRIES):
            try:
                # Perform bulk indexing and check if documents are indexed successfully
                failed = 0
                for ok, response in helpers.streaming_bulk(client=es_client, actions=actions, chunk_size=INT_CHUNK_SIZE):
                    if not ok:
                        failed += 1
                        # Log only the error message (not the full response object)
                        error_message = response.get('error', {}).get(
                            'reason', 'Unknown error')
                        logging.error(
                            f"Failed to index document: {error_message}")

                # If some documents failed, log a detailed message
                if failed > 0:
                    logging.error(
                        f"{failed} document(s) failed to index in this chunk.")

                # Exit the loop if indexing was successful (no failed documents)
                if failed == 0:
                    break
            except Exception as e:
                # Log the error and retry if necessary
                logging.error(
                    f"Error during bulk indexing to {index_name} (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)}")
                time.sleep(RETRY_DELAY)
        else:
            # If all attempts fail, log the failure
            logging.error(
                f"Failed to index chunk after {MAX_RETRIES} attempts.")

    except Exception as e:
        # Catching and logging any exceptions during the chunk processing
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
            # Create the index if it doesn't exist
            ensure_index_exists(index_name)
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
