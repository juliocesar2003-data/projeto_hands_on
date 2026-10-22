# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão API BACEN (Bronze)
# MAGIC
# MAGIC This notebook fetches currency exchange rate data and economic indicators from the Banco Central do Brasil (BACEN) API, writing raw NDJSON files to a Unity Catalog volume. It replaces the original Google Cloud Run + GCS ingestion architecture. The downstream Spark Declarative Pipeline will use Auto Loader to ingest these NDJSON files into bronze/silver/gold Delta tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS projeto_hands_on;
# MAGIC CREATE SCHEMA IF NOT EXISTS projeto_hands_on.bronze;
# MAGIC CREATE VOLUME IF NOT EXISTS projeto_hands_on.bronze.raw;

# COMMAND ----------

import requests
import json

# BACEN API configuration - 5 currency series
moedas = {
    "euro": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.21619/dados",
    "dolar": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados",
    "libra": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.21623/dados",
    "iene": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.21621/dados",
    "peso_argentino": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.21632/dados",
}

params = {
    "formato": "json",
    "dataInicial": "01/01/2016",
    "dataFinal": "31/12/2025",
}

volume_path = "/Volumes/projeto_hands_on/bronze/raw"
summary = {}

for moeda, url in moedas.items():
    print(f"Fetching {moeda} data from BACEN API...")
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    # Write as NDJSON (one JSON object per line)
    ndjson_content = "\n".join(json.dumps(record) for record in data)

    file_path = f"{volume_path}/{moeda}_2016_2025.json"
    dbutils.fs.put(file_path, ndjson_content, overwrite=True)

    summary[moeda] = len(data)
    print(f"  -> {len(data)} records written to {file_path}")

print("\n=== BACEN Ingestion Summary ===")
for moeda, count in summary.items():
    print(f"  {moeda}: {count} records")
print(f"  Total: {sum(summary.values())} records across {len(summary)} currencies")

# COMMAND ----------

import requests
import json
import time

# BACEN SGS economic indicator series
indicadores = {
    "selic": 432,
    "cdi": 12,
    "tr": 226,
    "ipca": 433,
}

params = {
    "formato": "json",
    "dataInicial": "01/01/2016",
    "dataFinal": "31/12/2025",
}

headers = {"User-Agent": "Mozilla/5.0 (Databricks; BACEN data ingestion)"}
volume_path = "/Volumes/projeto_hands_on/bronze/raw"
summary = {}

for name, code in indicadores.items():
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"
    print(f"Fetching {name} (code {code}) from BACEN API...")

    for attempt in range(3):
        response = requests.get(url, params=params, headers=headers, timeout=60)
        if response.status_code in (200, 404):
            break
        print(f"  -> HTTP {response.status_code}, retrying ({attempt + 1}/3)...")
        time.sleep(3)

    response.raise_for_status()
    data = response.json()

    ndjson_content = "\n".join(json.dumps(record) for record in data)
    file_path = f"{volume_path}/{name}_2016_2025.json"
    dbutils.fs.put(file_path, ndjson_content, overwrite=True)

    summary[name] = len(data)
    print(f"  -> {len(data)} records written to {file_path}")

print("\n=== BACEN Indicadores Ingestion Summary ===")
for name, count in summary.items():
    print(f"  {name}: {count} records")
print(f"  Total: {sum(summary.values())} records across {len(summary)} series")

# COMMAND ----------

volume_path = "/Volumes/projeto_hands_on/bronze/raw"

print("=== Files in volume ===")
files = dbutils.fs.ls(volume_path)
for f in files:
    print(f"  {f.name}  ({f.size} bytes)")

print(f"\nTotal files: {len(files)}")

# Read first 3 lines from dolar file
print("\n=== Sample: dolar_2016_2025.json (first 3 lines) ===")
with open(f"{volume_path}/dolar_2016_2025.json", "r") as f:
    for _ in range(3):
        line = f.readline()
        if line:
            print(f"  {line.strip()}")
