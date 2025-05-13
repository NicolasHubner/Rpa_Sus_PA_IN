from dotenv import load_dotenv
import os
load_dotenv()


# Replace with your Elasticsearch host
ELASTICSEARCH_HOST = "http://localhost:9200"
ES_INDEX_NAME_PREFIX = os.getenv('ES_INDEX_NAME_PREFIX')
CHUNK_SIZE = os.getenv('CHUNK_SIZE')  # Adjust the chunk size for bulk indexing
ELASTIC_USERNAME = os.getenv('ELASTIC_USERNAME')
ELASTIC_PASSWORD = os.getenv('DATABASE_ELASTIC_PASSWORD')

MAX_RETRIES = 3  # Max retry attempts for failed chunks
RETRY_DELAY = 5  # Delay (in seconds) between retry attempts
NUM_PROCESSES = 6  # Number of parallel processes
