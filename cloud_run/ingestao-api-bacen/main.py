import os
import requests
import json
from google.cloud import storage

def ingestao_multi_moedas_bronze(request):
    moedas = {
        "euro": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.21619/dados",
        "dolar": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.10813/dados",
        "libra": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.21623/dados",
        "iene": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.21621/dados",
        "peso_argentino": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.21632/dados"
    }

    nome_bucket = os.environ["BUCKET_NAME"]
    client = storage.Client()
    bucket = client.bucket(nome_bucket)

    params = {
        "formato": "json",
        "dataInicial": "01/01/2023",
        "dataFinal": "31/12/2025"
    }

    relatorio = []
    session = requests.Session()

    for moeda, url in moedas.items():
        try:
            response = session.get(url, params=params)

            if response.status_code == 200:
                registros = response.json()
                ndjson = "\n".join(json.dumps(r, ensure_ascii=False) for r in registros)

                caminho_gcs = f"bronze/{moeda}_2023_2025.json"
                blob = bucket.blob(caminho_gcs)
                blob.upload_from_string(
                    data=ndjson,
                    content_type="application/json"
                )
                relatorio.append(f"{moeda}: Sucesso")
            else:
                relatorio.append(f"{moeda}: Erro HTTP {response.status_code}")
        except Exception as e:
            relatorio.append(f"{moeda}: Falha na conexão ({str(e)})")

    return ", ".join(relatorio), 200
