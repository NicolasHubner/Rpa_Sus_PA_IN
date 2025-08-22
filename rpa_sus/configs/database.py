from dotenv import load_dotenv
import os
load_dotenv()


# Replace with your Elasticsearch host
ELASTICSEARCH_HOST = "https://localhost:9200"
ES_INDEX_NAME_PREFIX = os.getenv('ES_INDEX_NAME_PREFIX')
CHUNK_SIZE = 5000  # Increased chunk size for bulk indexing with 64GB RAM
ELASTIC_USERNAME = os.getenv('ELASTIC_USERNAME')
ELASTIC_PASSWORD = os.getenv('DATABASE_ELASTIC_PASSWORD')

MAX_RETRIES = 5  # Increased retry attempts for better reliability
RETRY_DELAY = 3  # Reduced delay for faster retries
NUM_PROCESSES = 8  # Use all 8 CPU cores
