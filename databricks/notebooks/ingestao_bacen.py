# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Ingestão API BACEN (Bronze) — Histórico Completo
# MAGIC
# MAGIC Este notebook busca cotações de câmbio e indicadores econômicos da API do Banco Central do Brasil (BACEN), cobrindo todo o histórico disponível desde a data mais antiga de cada série até 31/12/2025, e em seguida complementa com os dados de 01/01/2026 até a data de execução.
# MAGIC
# MAGIC **Carga única (one-off).** Depois desta carga, a atualização diária é feita pelo notebook `ingestao_diaria_bacen.py`, que continua a partir da última data gravada no volume sem repetir registros. Reexecutar este notebook apaga todos os arquivos do volume (inclusive os diários) e recria o histórico até hoje — nesse caso, o pipeline precisa de um *full refresh* para não duplicar dados na bronze.
# MAGIC
# MAGIC A API do BACEN permite no máximo 10 anos por consulta, então os dados são extraídos em janelas de 10 anos e consolidados em um único arquivo NDJSON por série, gravado em um volume do Unity Catalog. O pipeline Spark Declarative Pipeline usa Auto Loader para ingerir esses arquivos em tabelas Delta nas camadas bronze/silver.
# MAGIC
# MAGIC ## Data mais antiga por série
# MAGIC
# MAGIC | Série | Código | Data mais antiga |
# MAGIC |---|---|---|
# MAGIC | Dolar | 1 | 28/11/1984 |
# MAGIC | CDI | 12 | 06/03/1986 |
# MAGIC | IPCA | 433 | 01/01/1980 |
# MAGIC | TR | 226 | 01/02/1991 |
# MAGIC | Libra | 21623 | 01/07/1994 |
# MAGIC | Iene | 21621 | 01/07/1994 |
# MAGIC | Peso Argentino | 21632 | 01/07/1994 |
# MAGIC | Euro | 21619 | 31/12/1998 |
# MAGIC | Selic | 432 | 05/03/1999 |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS projeto_hands_on;
# MAGIC CREATE SCHEMA IF NOT EXISTS projeto_hands_on.bronze;
# MAGIC CREATE VOLUME IF NOT EXISTS projeto_hands_on.bronze.raw;

# COMMAND ----------

# DBTITLE 1,Ingest BACEN Currency Data
import requests
import json
import time
from datetime import datetime, timedelta

# BACEN API — 5 séries de câmbio com a data mais antiga disponível
moedas = {
    "euro": {"url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.21619/dados", "start": "01/01/1999"},
    "dolar": {"url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1/dados", "start": "01/01/1984"},
    "libra": {"url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.21623/dados", "start": "01/01/1994"},
    "iene": {"url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.21621/dados", "start": "01/01/1994"},
    "peso_argentino": {"url": "https://api.bcb.gov.br/dados/serie/bcdata.sgs.21632/dados", "start": "01/01/1994"},
}

END_DATE = "31/12/2025"
volume_path = "/Volumes/projeto_hands_on/bronze/raw"
headers = {"User-Agent": "Mozilla/5.0 (Databricks; BACEN data ingestion)"}

def gerar_janelas_10_anos(data_inicio, data_fim):
    """Divide um intervalo de datas em janelas de 10 anos."""
    inicio = datetime.strptime(data_inicio, "%d/%m/%Y")
    fim = datetime.strptime(data_fim, "%d/%m/%Y")
    janelas = []
    atual = inicio
    while atual < fim:
        try:
            proximo = atual.replace(year=atual.year + 10)
        except ValueError:
            proximo = atual.replace(year=atual.year + 10, day=28)
        if proximo > fim:
            proximo = fim
        janelas.append((atual.strftime("%d/%m/%Y"), proximo.strftime("%d/%m/%Y")))
        atual = proximo + timedelta(days=1)
    return janelas

# Limpar arquivos antigos do volume
print("=== Limpando arquivos antigos do volume ===")
for f in dbutils.fs.ls(volume_path):
    if f.name.endswith(".json"):
        dbutils.fs.rm(f.path)
        print(f"  Removido: {f.name}")

summary = {}

for moeda, config in moedas.items():
    print(f"\nBuscando {moeda} (de {config['start']} até {END_DATE})...")
    all_data = []
    janelas = gerar_janelas_10_anos(config["start"], END_DATE)

    for i, (inicio, fim) in enumerate(janelas):
        params = {"formato": "json", "dataInicial": inicio, "dataFinal": fim}
        print(f"  Janela {i+1}/{len(janelas)}: {inicio} a {fim}...", end=" ")

        for attempt in range(3):
            response = requests.get(config["url"], params=params, headers=headers, timeout=60)
            if response.status_code in (200, 404):
                break
            time.sleep(3)

        if response.status_code == 200:
            try:
                data = response.json()
                all_data.extend(data)
                print(f"{len(data)} registros")
            except (json.JSONDecodeError, ValueError):
                print("resposta inválida, tentando novamente...")
                time.sleep(5)
                response = requests.get(config["url"], params=params, headers=headers, timeout=60)
                try:
                    data = response.json()
                    all_data.extend(data)
                    print(f"{len(data)} registros (retry)")
                except (json.JSONDecodeError, ValueError):
                    print("resposta inválida, pulando janela")
        elif response.status_code == 404:
            print("sem dados (404)")
        else:
            print(f"HTTP {response.status_code}")

        time.sleep(2)

    ndjson_content = "\n".join(json.dumps(record) for record in all_data)
    start_year = config["start"].split("/")[-1]
    file_path = f"{volume_path}/{moeda}_{start_year}_2025.json"
    dbutils.fs.put(file_path, ndjson_content, overwrite=True)

    summary[moeda] = len(all_data)
    print(f"  -> Total: {len(all_data)} registros gravados em {file_path}")

print("\n=== Resumo da Ingestão — Moedas ===")
for moeda, count in summary.items():
    print(f"  {moeda}: {count} registros")
print(f"  Total: {sum(summary.values())} registros em {len(summary)} moedas")

# COMMAND ----------

# DBTITLE 1,Ingest BACEN Economic Indicators
import requests
import json
import time
from datetime import datetime, timedelta

# BACEN SGS — 4 indicadores econômicos com a data mais antiga disponível
indicadores = {
    "selic": {"code": 432, "start": "01/01/1999"},
    "cdi": {"code": 12, "start": "01/01/1986"},
    "tr": {"code": 226, "start": "01/01/1991"},
    "ipca": {"code": 433, "start": "01/01/1980"},
}

END_DATE = "31/12/2025"
volume_path = "/Volumes/projeto_hands_on/bronze/raw"
headers = {"User-Agent": "Mozilla/5.0 (Databricks; BACEN data ingestion)"}

def gerar_janelas_10_anos(data_inicio, data_fim):
    """Divide um intervalo de datas em janelas de 10 anos."""
    inicio = datetime.strptime(data_inicio, "%d/%m/%Y")
    fim = datetime.strptime(data_fim, "%d/%m/%Y")
    janelas = []
    atual = inicio
    while atual < fim:
        try:
            proximo = atual.replace(year=atual.year + 10)
        except ValueError:
            proximo = atual.replace(year=atual.year + 10, day=28)
        if proximo > fim:
            proximo = fim
        janelas.append((atual.strftime("%d/%m/%Y"), proximo.strftime("%d/%m/%Y")))
        atual = proximo + timedelta(days=1)
    return janelas

summary = {}

for name, config in indicadores.items():
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{config['code']}/dados"
    print(f"\nBuscando {name} (código {config['code']}, de {config['start']} até {END_DATE})...")
    all_data = []
    janelas = gerar_janelas_10_anos(config["start"], END_DATE)

    for i, (inicio, fim) in enumerate(janelas):
        params = {"formato": "json", "dataInicial": inicio, "dataFinal": fim}
        print(f"  Janela {i+1}/{len(janelas)}: {inicio} a {fim}...", end=" ")

        dados_janela = None
        for attempt in range(3):
            try:
                response = requests.get(url, params=params, headers=headers, timeout=60)
                if response.status_code == 200:
                    dados_janela = response.json()
                    break
                elif response.status_code == 404:
                    break
                else:
                    print(f"HTTP {response.status_code}", end=" ")
                    if attempt < 2:
                        print(f"tentando ({attempt+1}/3)...", end=" ")
                        time.sleep(3)
            except (requests.exceptions.RequestException, json.JSONDecodeError, ValueError):
                print(f"erro de conexão", end=" ")
                if attempt < 2:
                    print(f"tentando ({attempt+1}/3)...", end=" ")
                    time.sleep(5)

        if dados_janela:
            all_data.extend(dados_janela)
            print(f"{len(dados_janela)} registros")
        elif response.status_code == 404:
            print("sem dados (404)")
        else:
            print("falha")

        time.sleep(2)

    ndjson_content = "\n".join(json.dumps(record) for record in all_data)
    start_year = config["start"].split("/")[-1]
    file_path = f"{volume_path}/{name}_{start_year}_2025.json"
    dbutils.fs.put(file_path, ndjson_content, overwrite=True)

    summary[name] = len(all_data)
    print(f"  -> Total: {len(all_data)} registros gravados em {file_path}")

print("\n=== Resumo da Ingestão — Indicadores ===")
for name, count in summary.items():
    print(f"  {name}: {count} registros")
print(f"  Total: {sum(summary.values())} registros em {len(summary)} séries")

# COMMAND ----------

# DBTITLE 1,Ingest BACEN 2026 Until Today
import requests
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo

# Complemento do histórico: 01/01/2026 até hoje para as 9 séries (moedas + indicadores)
series_2026 = {
    "euro": 21619,
    "dolar": 1,
    "libra": 21623,
    "iene": 21621,
    "peso_argentino": 21632,
    "selic": 432,
    "cdi": 12,
    "tr": 226,
    "ipca": 433,
}

START_DATE = "01/01/2026"
hoje = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
END_DATE = hoje.strftime("%d/%m/%Y")
volume_path = "/Volumes/projeto_hands_on/bronze/raw"
headers = {"User-Agent": "Mozilla/5.0 (Databricks; BACEN data ingestion)"}

summary = {}
falhas = []

for name, code in series_2026.items():
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"
    params = {"formato": "json", "dataInicial": START_DATE, "dataFinal": END_DATE}
    print(f"\nBuscando {name} (código {code}, de {START_DATE} até {END_DATE})...", end=" ")

    dados = None
    sem_dados = False
    for attempt in range(3):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=60)
            if response.status_code == 200:
                resposta = response.json()
                if isinstance(resposta, list):
                    dados = resposta
                    break
                # A API às vezes responde "sem dados" com HTTP 200 e corpo {"erro": {"statusCode": 404, ...}}
                if isinstance(resposta, dict) and resposta.get("erro", {}).get("statusCode") == 404:
                    sem_dados = True
                    break
                print(f"resposta inesperada: {str(resposta)[:200]}", end=" ")
            elif response.status_code == 404:
                sem_dados = True
                break
            else:
                print(f"HTTP {response.status_code}", end=" ")
        except (requests.exceptions.RequestException, json.JSONDecodeError, ValueError):
            print("erro de conexão", end=" ")
        if attempt < 2:
            print(f"tentando ({attempt+1}/3)...", end=" ")
            time.sleep(5)

    if dados is None and not sem_dados:
        print("falha")
        falhas.append(name)
        continue

    # A API pode devolver registros fora do intervalo (ex.: série mensal antes de dataInicial,
    # Selic com datas futuras), então mantemos só 2026 até hoje
    inicio = datetime.strptime(START_DATE, "%d/%m/%Y").date()
    dados = [r for r in (dados or []) if inicio <= datetime.strptime(r["data"], "%d/%m/%Y").date() <= hoje]

    ndjson_content = "\n".join(json.dumps(record) for record in dados)
    file_path = f"{volume_path}/{name}_2026_{hoje.strftime('%Y%m%d')}.json"
    dbutils.fs.put(file_path, ndjson_content, overwrite=True)

    summary[name] = len(dados)
    print(f"{len(dados)} registros gravados em {file_path}")
    time.sleep(2)

print("\n=== Resumo da Ingestão — 2026 até hoje ===")
for name, count in summary.items():
    print(f"  {name}: {count} registros")
print(f"  Total: {sum(summary.values())} registros em {len(summary)} séries")

if falhas:
    raise RuntimeError(f"Falha ao buscar as séries: {', '.join(falhas)}. Reexecute esta célula.")

# COMMAND ----------

# DBTITLE 1,Verify Ingested Files
volume_path = "/Volumes/projeto_hands_on/bronze/raw"

print("=== Arquivos no volume ===")
files = dbutils.fs.ls(volume_path)
for f in files:
    print(f"  {f.name}  ({f.size} bytes)")

print(f"\nTotal de arquivos: {len(files)}")

# Ler as 3 primeiras linhas do arquivo do dolar
dolar_file = next((f.name for f in files if f.name.startswith("dolar_")), None)
if dolar_file:
    print(f"\n=== Amostra: {dolar_file} (primeiras 3 linhas) ===")
    with open(f"{volume_path}/{dolar_file}", "r") as f:
        for _ in range(3):
            line = f.readline()
            if line:
                print(f"  {line.strip()}")