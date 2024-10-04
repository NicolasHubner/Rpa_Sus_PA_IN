import pandas as pd
from elasticsearch import Elasticsearch,helpers
from multiprocessing import Pool
import logging
import os
import time

from configs.database import ELASTIC_PASSWORD, ELASTIC_USERNAME, CHUNK_SIZE, ELASTICSEARCH_HOST, MAX_RETRIES, RETRY_DELAY, NUM_PROCESSES, ES_INDEX_NAME_PREFIX

# CSV directory path
csv_directory = './data/csv'  # Specify the directory containing CSV files

# Create Elasticsearch client
es = Elasticsearch(
    [ELASTICSEARCH_HOST],
    basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
).options(request_timeout=60)

INT_CHUNK_SIZE = int(CHUNK_SIZE)

# Setup logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def chunk_records(records, chunk_size):
    """Yield successive chunks of a list."""
    logging.debug(f"Chunking records of type {type(records)}")
    if isinstance(records, list):  # Ensure records is a list before chunking
        for i in range(0, len(records), chunk_size):
            yield records[i:i + chunk_size]
    else:
        logging.error(f"Expected records to be a list, but got {type(records)} instead: {records}")
        return []

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
        
def read_csv_file(csv_file_path):
    """Reads a CSV file and handles various issues."""
    try:
        df = pd.read_csv(csv_file_path, on_bad_lines='skip')  # Skip bad lines
        logging.info(f"Successfully read {csv_file_path} with {len(df)} rows.")
        return df
    except pd.errors.EmptyDataError:
        logging.error(f"No data found in {csv_file_path}.")
    except pd.errors.ParserError as e:
        logging.error(f"Error parsing {csv_file_path}: {str(e)}")
    except Exception as e:
        logging.error(f"Failed to read CSV file {csv_file_path}: {str(e)}")
    return None

def prepare_es_doc(record, index_name):
    """Prepare a document for Elasticsearch from a CSV record."""
    try:
        return {
            '_index': index_name,
            '_source': record
        }
    except Exception as e:
        logging.error(f"Error preparing document for index {index_name}: {str(e)}")
        return None

def parallel_bulk_index(csv_directory, num_processes=NUM_PROCESSES):
    """Processes CSV files in parallel and indexes them in Elasticsearch."""
    csv_files = [f for f in os.listdir(csv_directory) if f.endswith('.csv')]
    
    with Pool(processes=num_processes) as pool:
        for csv_file in csv_files:
            index_name = f'{ES_INDEX_NAME_PREFIX}{os.path.splitext(csv_file)[0]}'.lower().replace(' ', '_').replace('-', '_')
            csv_file_path = os.path.join(csv_directory, csv_file)
            
            df = read_csv_file(csv_file_path)
            if df is not None:
                records = df.to_dict(orient='records')

                # Debugging check
                logging.debug(f"Type of 'records': {type(records)}")
                if isinstance(records, list):
                    logging.debug(f"'records' is a list with {len(records)} elements.")
                else:
                    logging.error(f"'records' is not a list. Actual content: {records}")

                # Ensure 'records' is a list of dictionaries
                if isinstance(records, list) and all(isinstance(item, dict) for item in records):
                    chunked_records = list(chunk_records(records, INT_CHUNK_SIZE))
                    logging.debug(f"Number of chunks created: {len(chunked_records)}")
                    pool.starmap(process_chunk, [(chunk, index_name) for chunk in chunked_records])
                else:
                    logging.error(f"Failed to process records for {csv_file}. Expected a list of dicts, but got {type(records)}")
            else:
                logging.error(f"Failed to process {csv_file}")

if __name__ == '__main__':
    csv_directory = './data/csv'
    parallel_bulk_index(csv_directory)