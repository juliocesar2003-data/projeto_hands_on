import os
import requests
import json
from google.cloud import storage

def ingestao_vendas_bronze(request):
    url = "https://dummyjson.com/carts"

    response = requests.get(url, timeout=15)

    if response.status_code == 200:
        carts = response.json()["carts"]
        ndjson = "\n".join(json.dumps(c, ensure_ascii=False) for c in carts)

        nome_bucket = os.environ["BUCKET_NAME"]
        caminho_gcs = "bronze/vendas_usd.json"

        client = storage.Client()
        bucket = client.bucket(nome_bucket)
        blob = bucket.blob(caminho_gcs)

        blob.upload_from_string(
            data=ndjson,
            content_type="application/json"
        )

        return f"Sucesso! Vendas salvas em gs://{nome_bucket}/{caminho_gcs}", 200
    else:
        return f"Erro ao acessar a API: {response.status_code}", 500
