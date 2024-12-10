from elasticsearch import Elasticsearch, AsyncElasticsearch

from ..configs.database import ELASTIC_PASSWORD, ELASTIC_USERNAME, ELASTICSEARCH_HOST


es = Elasticsearch(
    [ELASTICSEARCH_HOST],
    basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
).options(request_timeout=60)

es_async = AsyncElasticsearch(
    [ELASTICSEARCH_HOST],
    basic_auth=(ELASTIC_USERNAME, ELASTIC_PASSWORD),
).options(request_timeout=60)