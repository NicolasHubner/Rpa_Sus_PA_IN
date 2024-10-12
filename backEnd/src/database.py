from elasticsearch import Elasticsearch

from configs.database import ELASTIC_PASSWORD, ELASTIC_USERNAME, ELASTICSEARCH_HOST


es = Elasticsearch(
    [ELASTICSEARCH_HOST],
    basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
).options(request_timeout=60)
