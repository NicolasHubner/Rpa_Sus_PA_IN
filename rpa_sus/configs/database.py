from dotenv import load_dotenv
# import multiprocessing

import os
load_dotenv()


ELASTICSEARCH_HOST = os.getenv('ELASTICSEARCH_HOST')
ES_INDEX_NAME_PREFIX = os.getenv('ES_INDEX_NAME_PREFIX')
CHUNK_SIZE = os.getenv('CHUNK_SIZE')  # Adjust the chunk size for bulk indexing
ELASTIC_USERNAME = os.getenv('ELASTIC_USERNAME')
ELASTIC_PASSWORD = os.getenv('DATABASE_ELASTIC_PASSWORD')

MAX_RETRIES = 3  # Max retry attempts for failed chunks
RETRY_DELAY = 5  # Delay (in seconds) between retry attempts
NUM_PROCESSES = 4  # Number of parallel processes
