import requests

ES_URL = "http://localhost:9200"   # troque para o endereço do seu cluster
AUTH = ("elastic", "nicolasprojetoflavio22")        # se não tiver auth, coloque None

def main():
    # 1. Pegar todos os índices ordenados pela data de criação (mais recentes primeiro)
    resp = requests.get(
    f"{ES_URL}/_cat/indices?h=index&s=creation.date:desc",
    auth=AUTH,
    verify="./certs/ca/ca.crt"
)

    if resp.status_code != 200:
        print("Erro ao listar índices:", resp.text)
        return

    indices = resp.text.strip().split("\n")
    selected = indices[:165]  # só os 165 últimos

    for index in selected:
        new_index = f"{index}_sao_paulo"
        print(f"Reindexando {index} -> {new_index}")

        payload = {
            "source": {"index": index},
            "dest": {"index": new_index}
        }

        r = requests.post(f"{ES_URL}/_reindex", json=payload, auth=AUTH)
        if r.status_code not in (200, 201):
            print(f"Falhou {index}: {r.status_code} {r.text}")
        else:
            print("OK:", r.json())

if __name__ == "__main__":
    main()
