# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Ingestão do Boletim Focus (Bronze) — Incremental
# MAGIC
# MAGIC Busca na API de Expectativas do Banco Central as expectativas de mercado para o IPCA mensal e grava os registros novos em NDJSON no volume `projeto_hands_on.bronze.raw_focus`.
# MAGIC
# MAGIC | Arquivo | Origem | O que guarda |
# MAGIC |---|---|---|
# MAGIC | `focus_mensal_<timestamp>.json` | `ExpectativaMercadoMensais`, `baseCalculo = 0` | Expectativas de todas as instituições, nos últimos 30 dias |
# MAGIC | `focus_top5_<timestamp>.json` | `ExpectativasMercadoTop5Mensais`, `tipoCalculo = C` | Expectativas das cinco instituições que mais acertam no curto prazo |
# MAGIC
# MAGIC ## Como evita repetição
# MAGIC
# MAGIC - A última data de pesquisa de cada fonte é lida dos próprios arquivos do volume, e só datas posteriores são buscadas. Na primeira execução, sem arquivos, vem todo o histórico (desde 2000 no Focus e 2001 no Top 5).
# MAGIC - Cada execução grava um arquivo novo por fonte, apenas se houver registros novos, e nunca sobrescreve arquivos.
# MAGIC - Se uma fonte falhar, a outra é gravada normalmente, e o notebook termina com erro. A próxima execução recupera o período pendente.

# COMMAND ----------

# DBTITLE 1,Parameters
# O teste em sandbox usa outro volume para não tocar nos dados de produção
dbutils.widgets.text("volume", "projeto_hands_on.bronze.raw_focus", "Volume de destino (catálogo.schema.volume)")

# COMMAND ----------

# DBTITLE 1,Configuration
import json
import time
from datetime import datetime
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo

import requests
from pyspark.sql.functions import max as spark_max

VOLUME = dbutils.widgets.get("volume").strip()
VOLUME_PATH = "/Volumes/" + VOLUME.replace(".", "/")
URL_BASE = "https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/"
HEADERS = {"User-Agent": "Mozilla/5.0 (Databricks; BACEN data ingestion)"}
TAMANHO_PAGINA = 50000

# Prefixo do arquivo -> (recurso da API, filtro). Os prefixos precisam bater com os globs do pipeline
FONTES = {
    "focus_mensal": ("ExpectativaMercadoMensais", "Indicador eq 'IPCA' and baseCalculo eq 0"),
    "focus_top5": ("ExpectativasMercadoTop5Mensais", "Indicador eq 'IPCA' and tipoCalculo eq 'C'"),
}

spark.sql(f"CREATE VOLUME IF NOT EXISTS {VOLUME}")
agora = datetime.now(ZoneInfo("America/Sao_Paulo"))
print(f"Execução em {agora.strftime('%d/%m/%Y %H:%M:%S')} (America/Sao_Paulo)")

# COMMAND ----------

# DBTITLE 1,Ingest New Records
def montar_filtro(filtro, ultima_data):
    """Filtro OData da fonte, restrito às pesquisas posteriores à última data gravada (texto AAAA-MM-DD), se houver."""
    return filtro if ultima_data is None else f"{filtro} and Data gt '{ultima_data}'"


def buscar_pagina(url, params):
    """Busca uma página da API. Retorna a lista de registros ou None em caso de falha."""
    for attempt in range(3):
        try:
            # A API só aceita espaços como %20 na consulta, então a URL é montada aqui (o requests usaria +)
            response = requests.get(f"{url}?{urlencode(params, quote_via=quote)}", headers=HEADERS, timeout=180)
            if response.status_code == 200:
                return response.json()["value"]
            print(f"HTTP {response.status_code}", end=" ")
        except (requests.exceptions.RequestException, json.JSONDecodeError, ValueError, KeyError):
            print("erro de conexão", end=" ")
        if attempt < 2:
            print(f"tentando ({attempt+1}/3)...", end=" ")
            time.sleep(5 * (attempt + 1))
    return None


def buscar_registros(recurso, filtro, tamanho_pagina=TAMANHO_PAGINA):
    """Busca todas as páginas de um recurso. Retorna a lista de registros ou None se alguma página falhar."""
    registros, pulo = [], 0
    while True:
        params = {"$filter": filtro, "$format": "json", "$orderby": "Data asc", "$top": tamanho_pagina, "$skip": pulo}
        pagina = buscar_pagina(URL_BASE + recurso, params)
        if pagina is None:
            return None
        registros += pagina
        if len(pagina) < tamanho_pagina:
            return registros
        pulo += tamanho_pagina


def filtrar_novos(registros, ultima_data):
    """Mantém só as pesquisas posteriores à última data gravada, sem registros repetidos."""
    vistos, filtrados = set(), []
    for registro in registros:
        chave = json.dumps(registro, sort_keys=True)
        if (ultima_data is None or registro["Data"] > ultima_data) and chave not in vistos:
            vistos.add(chave)
            filtrados.append(registro)
    return filtrados


arquivos = [f.name for f in dbutils.fs.ls(VOLUME_PATH)]
falhas = []
for fonte, (recurso, filtro) in FONTES.items():
    ultima_data = None
    if any(a.startswith(f"{fonte}_") and a.endswith(".json") for a in arquivos):
        ultima_data = spark.read.json(f"{VOLUME_PATH}/{fonte}_*.json").agg(spark_max("Data")).first()[0]
    print(f"\n{fonte}: última pesquisa gravada {ultima_data}")

    registros = buscar_registros(recurso, montar_filtro(filtro, ultima_data))
    if registros is None:
        print("  -> falha")
        falhas.append(fonte)
        continue

    novos = filtrar_novos(registros, ultima_data)
    if novos:
        caminho = f"{VOLUME_PATH}/{fonte}_{agora.strftime('%Y%m%d_%H%M%S')}.json"
        with open(caminho, "x", encoding="utf-8") as arquivo:
            arquivo.write("\n".join(json.dumps(registro) for registro in novos))
        print(f"  -> {len(novos)} registros novos gravados em {caminho}")
    else:
        print("  -> nenhum registro novo")

if falhas:
    raise RuntimeError(
        f"Falha nas fontes: {', '.join(falhas)}. A próxima execução recupera o período pendente automaticamente."
    )
