import pandas as pd
from elasticsearch import Elasticsearch, helpers
from multiprocessing import Pool
import logging
import os
import time
from configs.database import ELASTIC_PASSWORD, ELASTIC_USERNAME, CHUNK_SIZE, ELASTICSEARCH_HOST, MAX_RETRIES, RETRY_DELAY, NUM_PROCESSES, ES_INDEX_NAME_PREFIX

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG to capture all logs
    format='%(asctime)s - %(levelname)s - %(message)s'  # Format the log messages
)

# Initialize Elasticsearch client
es = Elasticsearch(
    [ELASTICSEARCH_HOST],
    basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
    request_timeout=60
)


# Reduce the verbosity of the Elasticsearch client
logging.getLogger('elasticsearch').setLevel(logging.WARNING)

# Constants
INT_CHUNK_SIZE = 5000

def read_csv_with_semicolon(filepath):
    try:
        # Ensure we are reading the file with the proper delimiter
        df = pd.read_csv(filepath, sep=';', dtype={'column1': str, 'column14': str, 'column15': str, 'column16': str}, low_memory=False)
        
        # Replace all NaN values with None (to be converted to null in JSON)
        df = df.where(pd.notnull(df), None)
        
        if isinstance(df, pd.DataFrame):
            logging.info(f"Successfully read {filepath} with {len(df)} rows.")
            return df
        else:
            logging.error(f"Failed to read {filepath} as a DataFrame.")
            return None
    except Exception as e:
        logging.error(f"Error reading {filepath}: {e}")
        return None

def chunk_records(records, chunk_size):
    """Yields chunks of records."""
    logging.debug(f"Chunking {len(records)} records into size {chunk_size}")
    for i in range(0, len(records), chunk_size):
        yield records[i:i + chunk_size]

import numpy as np  # Make sure to import NumPy if you're using it

def prepare_es_doc(record, index_name):
    """Prepare a single document for Elasticsearch."""
    try:
        # Replace NaN values with None
        record = {k: (v if not isinstance(v, float) or not np.isnan(v) else None) for k, v in record.items()}
        
        return {
            '_index': index_name,
            '_source': record
        }
    except Exception as e:
        logging.error(f"Error preparing document: {e}")
        return None


def process_chunk(data_chunk, index_name):
    """Indexes a chunk of data into Elasticsearch with retries."""
    actions = [prepare_es_doc(record, index_name) for record in data_chunk]
    
    # Log the size of the chunk being processed
    logging.info(f"Processing chunk of size {len(data_chunk)} for index {index_name}")

    for attempt in range(MAX_RETRIES):
        try:
            # Log only when sending a chunk
            logging.info(f"Sending chunk of {len(data_chunk)} records to index {index_name}")
            
            # Use the helpers.bulk and capture the response
            success, failed = helpers.bulk(es, actions, chunk_size=CHUNK_SIZE, raise_on_error=False)
            
            if failed:
                logging.error(f"Some documents failed to index on attempt {attempt + 1}: {failed}")
            
            logging.info(f"Successfully indexed {success} documents.")
            break
        except Exception as e:
            logging.error(f"Error during bulk indexing (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            time.sleep(RETRY_DELAY)
    else:
        logging.error(f"Failed to index after {MAX_RETRIES} attempts.")



def parallel_bulk_index(csv_directory):
    """Reads CSVs from directory, chunks data, and indexes in parallel."""
    csv_files = [f for f in os.listdir(csv_directory) if f.endswith('.csv')]
    if not csv_files:
        logging.error("No CSV files found in the directory.")
        return

    with Pool(processes=NUM_PROCESSES) as pool:
        for csv_file in csv_files:
            index_name = f"{ES_INDEX_NAME_PREFIX}{os.path.splitext(csv_file)[0]}".lower().replace(' ', '_').replace('-', '_')
            csv_file_path = os.path.join(csv_directory, csv_file)

            df = read_csv_with_semicolon(csv_file_path)
            if df is not None:
                records = df.to_dict(orient='records')
                chunked_records = list(chunk_records(records, INT_CHUNK_SIZE))
                logging.debug(f"Created {len(chunked_records)} chunks for {csv_file}")
                
                pool.starmap(process_chunk, [(chunk, index_name) for chunk in chunked_records])

if __name__ == '__main__':
    csv_directory = './data/csv'
    parallel_bulk_index(csv_directory)
