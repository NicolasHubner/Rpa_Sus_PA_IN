from fastapi import HTTPException
from ..database import es, es_async

# 2. Rota para upload dos arquivos, pensar em algo no máx de 10gb
# I can update more than 1 file per time, but max 10gb
# Need to create a new index for files
def upload_file(file):
    try:
        es.index(index="files", id=file.filename, body=file.file)
    except:
        raise HTTPException(status_code=400, detail="File too big")
    return file 


# # 3. Rota de Busca rapida(colocar 50 elementos)
async def search_database():
    # GET _cat/indices/data_*?h=index,docs.count,store.size,health,status
    try:
        response = await es_async.cat.indices(
            index="index_*",
            v=True,
            format="json",
            s="index",
        )
        if not response:
            return {"error": "Resposta vazia do Elasticsearch"}
        return response
    except Exception as e:
        return {"error": str(e)}


# 4. Rota de Busca de Estado/Mês/Ano Mostrar Tnato PA/RD - Qtd de Registro - Total de gasto
async def search_state_database():
    ano = 2018
    mes = 5
    uf = 12 # trocar para sigla

    # todo melhorar index
    response = await es_async.search(index="index_*", size=0, query={
    "bool": {
      "filter": [
        {
          "term": {
            "ANO": 2008
          }
        },
        {
          "term": {
            "MES": 6
          }
        }
      ],
      "should": [
        {
          "prefix": {
            "PA_UFMUN": "12"
          }
        },
        {
          "prefix": {
            "UF_ZI": "12"
          }
        }
      ],
      "minimum_should_match": 1
        }
    }, 
    aggregations={
        "total_documents": {
        "value_count": {
            "field": "TIPO.keyword"
        }
        },
        "total_sum": {
        "sum": {
            "field": "TOTAL"
        }
        },
        "tipos": {
        "terms": {
            "field": "TIPO.keyword"
        },
        "aggs": {
            "total_documents": {
            "value_count": {
                "field": "TIPO.keyword"
            }
            },
            "total_sum": {
            "sum": {
                "field": "TOTAL"
            }
            }
        }
        }
    }
    )
    return response

    # return es.search(index="users", body={"query": {"match": {"state": state, "month": month, "year": year}}})