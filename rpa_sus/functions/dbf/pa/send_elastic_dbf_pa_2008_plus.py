# fmt: off
# Add the project root directory to Python path
import sys
import os
project_root = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..', '..', '..'))
sys.path.insert(0, project_root)

import logging
import time
import threading
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
import pandas as pd

import random
import hashlib

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

dbf_directory = '/mnt/volume_nyc1_01/nicolas/PA/Ceara'

INT_CHUNK_SIZE = int(CHUNK_SIZE)

# Enable detailed error analysis (set to False to reduce log verbosity)
ENABLE_DETAILED_ERROR_ANALYSIS = True

COLUNS_TO_WATCH_PA_2008_PLUS = [
    "PA_CODUNI",
    "PA_GESTAO",
    "PA_UFMUN",
    "PA_CNPJCPF",
    "PA_CNPJMNT",
    "PA_MVM",
    "PA_CMP",
    "PA_PROC_ID",
    "PA_TPFIN",
    "PA_SUBFIN",
    "PA_CBOCOD",
    "PA_CIDPRI",
    "PA_QTDPRO",
    "PA_QTDAPR",
    "PA_VALPRO",
    "PA_VALAPR",
]


# Initialize global counter to track processed records
processed_records_count = 0

# Global set to track created indices (to avoid recreating them)
created_indices = set()
indices_lock = threading.Lock()

MAX_BACKOFF = 60  # Maximum backoff time in seconds

# Create Elasticsearch client with comprehensive error handling
try:
    es_client = Elasticsearch(
        [ELASTICSEARCH_HOST],
        basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
        retry_on_timeout=True,
        verify_certs=False,  # Disable SSL certificate verification
        max_retries=MAX_RETRIES,
    ).options(request_timeout=120)  # Increase request timeout to 120 seconds

    # Check if the connection is successful
    if es_client.ping():
        logging.info("Connected to Elasticsearch!")

except Exception as connection_error:
    logging.error(f"Elasticsearch connection failed: {connection_error}")
    logging.error(f"Traceback: {traceback.format_exc()}")
    sys.exit(1)
except Exception as connection_error:
    logging.error(f"Elasticsearch connection failed: {connection_error}")
    logging.error(f"Traceback: {traceback.format_exc()}")
    sys.exit(1)

# Precomputed mapping for state codes
code_to_state = {value: key for key, value in state_codes.items()}

# Function to generate a unique document ID based on multiple fields
def generate_document_id(record):
    """Generate a unique document ID based on multiple fields."""

    
    # Select fields that together make a record unique
    # Adjust these fields based on your data structure
    unique_fields = [
        record.get("PA_CODUNI", ""),
        record.get("PA_GESTAO", ""),
        record.get("PA_UFMUN", ""),
        record.get("PA_DATPR", ""),
        record.get("PA_DATREF", ""),
        record.get("PA_CODPRO", ""),
        record.get("PA_CIDPRI", ""),
        record.get("PA_QTDPRO", ""),
        record.get("PA_QTDAPR", ""),
        record.get("PA_VALPRO", ""),
        record.get("PA_VALAPR", ""),
    ]
    
    # Join the fields with a separator
    base_id = "_".join(str(field) for field in unique_fields if field)
    
    # Add a random component to ensure uniqueness
    random_component = str(random.randint(1000, 99999999))

    # Combine the base ID with the random component
    combined_id = f"{base_id}_{random_component}"
    
   # Optionally, you can hash the combined ID for a fixed-length ID
    # This is useful if your IDs might get too long
    hashed_id = hashlib.md5(combined_id.encode()).hexdigest()
    
    # Return the unique ID with random component
    return hashed_id


def get_mapped_value(code, mapping, default="Unknown"):
    """Helper function to get mapped value from a dictionary."""
    try:
        return mapping.get(int(code), default)
    except ValueError:
        return default

def get_mapped_name_state(code, mapping, default="Unknown"):
    """Helper function to get state name from a code where only the first two digits matter."""
    try:
        # Convert to string if it's not already
        code_str = str(code).strip()
        
        # Extract the first two digits if the code is long enough
        if len(code_str) >= 2:
            state_code = code_str[:2]  # Get first two digits
            return get_mapped_value(state_code, mapping, default)
        else:
            return default
    except ValueError:
        return default

def handle_date_conversion(date_value):
    """
    Convert a date value in YYYYMM format to YYYY-MM-DDTHH:MM:SS format.
    
    Args:
        date_value: A date in YYYYMM format (as int or string)
        
    Returns:
        str: Date in YYYY-MM-DDTHH:MM:SS format, or None if conversion fails
    """
    try:
        # Convert to string and ensure proper length
        date_str = str(date_value).zfill(6)
        # Parse the date and set day to 15th of the month
        dt = datetime.strptime(date_str, "%Y%m").replace(day=15)
        # Format to the desired output
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except (ValueError, TypeError):
        return None


def save_failed_documents_sample(errors, chunk_index, dbf_file, actions):
    """Save a sample of failed documents to a file for analysis."""
    try:
        import json
        from datetime import datetime
        
        # Create a filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"failed_docs_{dbf_file}_{chunk_index}_{timestamp}.json"
        filepath = os.path.join(os.path.dirname(__file__), 'error_logs', filename)
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        # Collect failed document info
        failed_docs_info = []
        
        for error in errors[:10]:  # Save first 10 failed documents
            doc_id = error.get('index', {}).get('_id', 'unknown')
            error_info = error.get('index', {}).get('error', {})
            
            # Find the corresponding document in actions
            failed_doc = None
            for action in actions:
                if action.get('_id') == doc_id:
                    failed_doc = action.get('_source', {})
                    break
            
            failed_docs_info.append({
                'document_id': doc_id,
                'error_type': error_info.get('type', 'unknown'),
                'error_reason': error_info.get('reason', 'unknown'),
                'error_status': error.get('index', {}).get('status', 'unknown'),
                'document_data': failed_doc
            })
        
        # Save to file
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump({
                'file': dbf_file,
                'chunk': chunk_index,
                'timestamp': timestamp,
                'total_errors': len(errors),
                'failed_documents_sample': failed_docs_info
            }, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Failed documents sample saved to: {filepath}")
        
    except Exception as e:
        logging.error(f"Failed to save failed documents sample: {str(e)}")


def analyze_bulk_errors(errors, chunk_index, dbf_file):
    """Analyze and categorize bulk indexing errors."""
    error_summary = {}
    sample_errors = []
    
    for error in errors:
        error_info = error.get('index', {}).get('error', {})
        error_type = error_info.get('type', 'unknown')
        error_reason = error_info.get('reason', 'unknown')
        
        # Categorize errors
        if error_type not in error_summary:
            error_summary[error_type] = {
                'count': 0,
                'sample_reason': error_reason,
                'sample_doc_id': error.get('index', {}).get('_id', 'unknown')
            }
        error_summary[error_type]['count'] += 1
        
        # Keep sample errors for detailed logging
        if len(sample_errors) < 3:
            sample_errors.append({
                'type': error_type,
                'reason': error_reason,
                'doc_id': error.get('index', {}).get('_id', 'unknown'),
                'status': error.get('index', {}).get('status', 'unknown')
            })
    
    # Log error summary
    logging.error(f"Error Analysis for chunk {chunk_index} in file '{dbf_file}':")
    logging.error(f"Total errors: {len(errors)}")
    
    for error_type, info in error_summary.items():
        logging.error(f"  {error_type}: {info['count']} occurrences")
        logging.error(f"    Sample reason: {info['sample_reason']}")
        logging.error(f"    Sample doc ID: {info['sample_doc_id']}")
    
    # Log detailed sample errors
    logging.error("Sample detailed errors:")
    for i, error in enumerate(sample_errors):
        logging.error(f"  Error {i+1}: [Status: {error['status']}] {error['type']}: {error['reason']}")
        logging.error(f"    Document ID: {error['doc_id']}")
    
    return error_summary


def validate_document(doc):
    """Validate a document before indexing to catch common issues."""
    try:
        source = doc.get('_source', {})
        
        # Check for required fields
        required_fields = ['PA_CODUNI', 'PA_UFMUN']
        for field in required_fields:
            if field not in source or source[field] is None or source[field] == '':
                return False, f"Missing or empty required field: {field}"
        
        # Check for document size (Elasticsearch has a 100MB limit per document)
        import json
        doc_size = len(json.dumps(source).encode('utf-8'))
        if doc_size > 50 * 1024 * 1024:  # 50MB warning threshold
            return False, f"Document too large: {doc_size} bytes"
        
        # Check for field value lengths (some fields might be too long)
        for key, value in source.items():
            if value is not None and isinstance(value, str) and len(value) > 32766:
                return False, f"Field '{key}' value too long: {len(value)} characters"
        
        # Check for valid numeric fields
        numeric_fields = ['PA_QTDPRO', 'PA_QTDAPR', 'PA_VALPRO', 'PA_VALAPR', 'VAL_GERAL']
        for field in numeric_fields:
            if field in source and source[field] is not None:
                try:
                    float(source[field])
                except (ValueError, TypeError):
                    return False, f"Invalid numeric value in field '{field}': {source[field]}"
        
        return True, "Valid"
        
    except Exception as e:
        return False, f"Validation error: {str(e)}"


def prepare_es_doc(record, index_name, fields_to_include=COLUNS_TO_WATCH_PA_2008_PLUS):
    """Prepare a document for Elasticsearch by cleaning and transforming data."""
    # Step 1: Clean the record by preprocessing columns
    cleaned_record = clean_column_data(record)
        
    # Step 2: Filter the record to include only specified fields (optional)
    if fields_to_include:
        cleaned_record = {
            k: v for k, v in cleaned_record.items() if k in fields_to_include}

    # Step 3: Handle PA_CMP conversion to yyyyMM format
    pa_cmp = cleaned_record.get("PA_CMP", "")
    if pa_cmp:
        date_value = handle_date_conversion(pa_cmp)
        if date_value:
            # Use the datetime value
            formatted_date = date_value  # Format as needed
        else:
            # Handle the case where conversion wasn't possible
            formatted_date = None
        cleaned_record["@DATA"] = formatted_date  # Update the field

    # Step 4: Generate a unique document ID
    doc_id = generate_document_id(cleaned_record)
    
    # COMUM PARA TODOS
    # Step 4: Handle VAL_GERAL
    cleaned_record["VAL_GERAL"] = cleaned_record['PA_VALAPR']

    # Step 5: Handle CNPJCPF - Convert PA_CODUNI to CNPJ
    pa_cnpj_cpf = cleaned_record.get("PA_CNPJCPF", "")

    cleaned_record["CNPJ_CPF"] = pa_cnpj_cpf

    # Step 7: Handle Source
    cleaned_record["SOURCE"] = 'SIA'

    # Step 8: Prepare the final document for Elasticsearch
    return {
        '_index': index_name,
        '_id': doc_id,  # Add the unique document ID
        '_source': {k: str(v) if v is not None else None for k, v in cleaned_record.items()},
    }


def safe_ensure_index_exists(index_name: str):
    """
    Simple wrapper - just try to create the index and continue regardless.
    """
    # Quick check without lock
    if index_name in created_indices:
        return True
    
    try:
        result = ensure_index_exists(index_name)
        if result:
            with indices_lock:
                created_indices.add(index_name)
        return result
    except Exception as e:
        logging.warning(f"Index creation failed for '{index_name}': {e}")
        return False


def ensure_index_exists(index_name: str):
    """
    Simple and reliable index creation - no retries, no complex logic.
    Just try to create and ignore if it already exists.
    """
    try:
        # Simple approach: just try to create with ignore=400
        es_client.indices.create(
            index=index_name,
            body={
                "settings": {
                    "number_of_shards": 2,  # Increased shards for better performance
                    "number_of_replicas": 0,  # Keep replicas at 0 for faster indexing
                    "refresh_interval": "60s",  # Increased refresh interval for better performance
                    "index.max_result_window": 100000,  # Increased for larger result sets
                    "index.mapping.total_fields.limit": 2000,  # Increased field limit
                    "index.max_docvalue_fields_search": 200,  # Increased for better aggregations
                }
            },
            ignore=[400, 404],  # Ignore "already exists" and "not found" errors
            timeout='15s'  # Increased timeout
        )
        logging.info(f"Index '{index_name}' created or already exists.")
        return True
        
    except Exception as e:
        logging.warning(f"Could not create index '{index_name}': {e}")
        # Try even simpler creation
        try:
            es_client.indices.create(index=index_name, ignore=400, timeout='10s')  # Increased timeout
            logging.info(f"Simple index creation for '{index_name}' successful.")
            return True
        except Exception as e2:
            logging.error(f"All index creation failed for '{index_name}': {e2}")
            return False


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

        # Create actions with unique document IDs and validate them
        actions = []
        validation_errors = 0
        
        for record in valid_records:
            try:
                doc = prepare_es_doc(record, index_name)
                
                # Validate the document before adding to actions
                is_valid, validation_msg = validate_document(doc)
                if is_valid:
                    actions.append(doc)
                else:
                    validation_errors += 1
                    if validation_errors <= 3:  # Log first 3 validation errors
                        logging.warning(f"Document validation failed in chunk {chunk_index}: {validation_msg}")
                        logging.warning(f"Problematic record sample: {str(record)[:200]}...")
                    
            except Exception as e:
                validation_errors += 1
                if validation_errors <= 3:
                    logging.error(f"Error preparing document in chunk {chunk_index}: {str(e)}")
                    logging.error(f"Problematic record: {str(record)[:200]}...")
        
        if validation_errors > 0:
            logging.warning(f"Chunk {chunk_index}: {validation_errors} documents failed validation and were skipped")
        
        chunk_size = len(actions)

        logging.info(
            f"Processing chunk {chunk_index} for file '{dbf_file}' (Index: '{index_name}', Size: {chunk_size})")

        success_count = 0
        failed_documents = 0

        # Use a larger chunk size for streaming_bulk to take advantage of 64GB RAM
        streaming_chunk_size = min(500, INT_CHUNK_SIZE // 2)  # Larger chunks with more RAM

        start_time = time.time()

        # Reduced delay since we have more resources
        delay = random.uniform(0.5, 1.5)  # Shorter delay with better hardware
        time.sleep(delay)

        # Use helpers.bulk instead of streaming_bulk for better handling of document IDs
        try:
            # Use stats_only=False to get detailed error information
            results = helpers.bulk(
                client=es_client,
                actions=actions,
                chunk_size=streaming_chunk_size,
                max_retries=MAX_RETRIES,
                stats_only=False  # Get detailed results including errors
            )
            
            # Extract success and failed counts from results
            success_count = results[0]
            failed_documents = len(results[1]) if results[1] else 0
            
            # Log detailed error information if there are failures
            if failed_documents > 0:
                logging.error(f"Chunk {chunk_index} for file '{dbf_file}': {failed_documents} documents failed to index")
                for i, error_doc in enumerate(results[1][:5]):  # Log first 5 errors
                    error_info = error_doc.get('index', {})
                    error_reason = error_info.get('error', {})
                    logging.error(f"Error {i+1}: {error_reason}")
                    if hasattr(error_doc, '_source'):
                        logging.error(f"Failed document sample: {str(error_doc.get('_source', {}))[:200]}...")
                
                if len(results[1]) > 5:
                    logging.error(f"... and {len(results[1]) - 5} more errors")
            
        except helpers.BulkIndexError as e:
            # Handle BulkIndexError specifically to extract detailed error information
            logging.error(f"BulkIndexError in chunk {chunk_index} for file '{dbf_file}': {str(e)}")
            
            # Extract error details from the exception
            if hasattr(e, 'errors') and e.errors:
                # Use our analysis function to categorize and log errors
                error_summary = analyze_bulk_errors(e.errors, chunk_index, dbf_file)
                
                # Additional specific error checks
                mapping_errors = error_summary.get('mapper_parsing_exception', {}).get('count', 0)
                if mapping_errors > 0:
                    logging.error(f"Mapping errors detected: {mapping_errors}. This usually indicates data type mismatches.")
                
                version_conflicts = error_summary.get('version_conflict_engine_exception', {}).get('count', 0)
                if version_conflicts > 0:
                    logging.error(f"Version conflicts detected: {version_conflicts}. This might indicate duplicate document IDs.")
                
                resource_errors = error_summary.get('es_rejected_execution_exception', {}).get('count', 0)
                if resource_errors > 0:
                    logging.error(f"Resource/queue errors detected: {resource_errors}. Elasticsearch might be overloaded.")
            
            # Return 0 for failed processing
            return 0
            
        except exceptions.ConnectionError as e:
            if "429" in str(e) or "Too Many Requests" in str(e) or "circuit_breaking_exception" in str(e):
                # Handle 429 and circuit breaker errors with shorter delay for better hardware
                logging.warning(f"Rate limited or circuit breaker triggered, sleeping for 15 seconds before retry...")
                time.sleep(15)  # Reduced delay with better hardware
                # Retry once more with smaller chunk size
                try:
                    results = helpers.bulk(
                        client=es_client,
                        actions=actions,
                        chunk_size=streaming_chunk_size//2,  # Smaller chunk size for retry
                        max_retries=MAX_RETRIES,
                        stats_only=False
                    )
                    success_count = results[0]
                    failed_documents = len(results[1]) if results[1] else 0
                except helpers.BulkIndexError as retry_e:
                    logging.error(f"BulkIndexError on retry: {str(retry_e)}")
                    return 0
            else:
                raise

        duration = time.time() - start_time
        logging.info(
            f"Chunk {chunk_index} complete: {success_count} indexed, {failed_documents} failed in {duration:.2f}s")

        # Reduced delay after processing since we have better hardware
        time.sleep(0.2)  # Reduced delay with better resources

        return success_count

    except Exception as e:
        logging.error(
            f"Unexpected error processing chunk {chunk_index} for file '{dbf_file}' (Index: '{index_name}'): {str(e)}",
            exc_info=True
        )
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

    # Process files in batches of 12 to better utilize 64GB RAM
    batch_size = 12  # Increased batch size for better memory management with 64GB RAM
    
    # Calculate optimal worker count based on available system resources
    total_memory_gb = psutil.virtual_memory().total / (1024 * 1024 * 1024)
    cpu_count = os.cpu_count() or 1

    # Use more workers to take advantage of 8 CPU cores and 64GB RAM
    optimal_workers = min(cpu_count, 8)  # Use all 8 cores
    worker_count = max(4, optimal_workers)  # Minimum 4 workers, up to 8

    logging.info(f"System has {cpu_count} CPUs and {total_memory_gb:.1f}GB RAM")
    logging.info(f"Using {worker_count} worker processes for batch processing of {batch_size} files at a time")

    for batch_start in range(0, total_file_count, batch_size):
        batch_end = min(batch_start + batch_size, total_file_count)
        batch_files = dbf_files[batch_start:batch_end]

        logging.info(f"Processing batch {(batch_start//batch_size)+1}: files {batch_start+1}-{batch_end} of {total_file_count}")
        
        # Pre-create indices for this batch only
        batch_indices = []
        for dbf_file in batch_files:
            index_name = f"{ES_INDEX_NAME_PREFIX}{os.path.splitext(dbf_file)[0]}".lower(
            ).replace(" ", "_").replace("-", "_")
            batch_indices.append((dbf_file, index_name))
        
        # Quick index creation for this batch (non-blocking)
        logging.info(f"Quick index setup for batch of {len(batch_files)} files...")
        for dbf_file, index_name in batch_indices:
            # Just try once, don't block if it fails
            try:
                es_client.indices.create(index=index_name, ignore=400, timeout='5s')
                created_indices.add(index_name)
            except:
                pass  # Ignore all errors, will try during processing
        
        logging.info(f"Starting data processing for batch...")

        # Process files in this batch
        for dbf_file in batch_files:
            index_name = f"{ES_INDEX_NAME_PREFIX}{os.path.splitext(dbf_file)[0]}".lower(
            ).replace(" ", "_").replace("-", "_")
            dbf_file_path = os.path.join(dbf_directory, dbf_file)

            # Ensure index exists (quick attempt)
            if index_name not in created_indices:
                try:
                    es_client.indices.create(index=index_name, ignore=400, timeout='5s')
                    created_indices.add(index_name)
                    logging.info(f"Index created for '{dbf_file}'")
                except Exception as ie:
                    logging.warning(f"Index creation failed for '{dbf_file}': {ie}")
                    # Continue anyway - let Elasticsearch handle it during bulk indexing

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
                        
                        # Reduced delay between chunk submissions with better hardware
                        if file_chunks % 5 == 0:  # Every 5 chunks, add a short delay
                            time.sleep(1.0)  # Shorter delay
                        else:
                            time.sleep(0.2)  # Reduced base delay

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

                # Reduced delay between files with better hardware
                time.sleep(2.0)  # Reduced delay between files

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
        
        batch_num = (batch_start // batch_size) + 1
        total_batches = (total_file_count + batch_size - 1) // batch_size
        
        logging.info("=" * 60)
        logging.info(f"BATCH {batch_num}/{total_batches} COMPLETED")
        logging.info(f"Files processed in this batch: {len(batch_files)}")
        logging.info(f"Total files processed so far: {batch_end}/{total_file_count}")
        logging.info(f"Total records indexed so far: {total_processed}")
        logging.info(f"Time elapsed: {current_minutes}m {current_seconds:.1f}s")
        
        if batch_num < total_batches:
            estimated_remaining = (current_elapsed / batch_end) * (total_file_count - batch_end)
            est_minutes = int(estimated_remaining // 60)
            est_seconds = estimated_remaining % 60
            logging.info(f"Estimated time remaining: {est_minutes}m {est_seconds:.1f}s")
        
        logging.info("=" * 60)
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
        logging.error(f"Unexpected error EXECPTION ERROR RONALDO: {e}", exc_info=True)
    finally:
        logging.info("Script execution has ended.")
        # Optionally, you can add a cleanup function here if needed
