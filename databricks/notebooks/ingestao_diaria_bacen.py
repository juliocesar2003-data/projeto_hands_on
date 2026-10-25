# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Ingestão Diária API BACEN (Bronze) — Incremental
# MAGIC
# MAGIC Este notebook é executado diariamente por um Job agendado e busca na API do BACEN apenas os registros novos de cada série, desde a última data já gravada no volume até hoje.
# MAGIC
# MAGIC ## Como evita repetição
# MAGIC
# MAGIC - A última data de cada série é lida diretamente dos arquivos NDJSON do volume `projeto_hands_on.bronze.raw` (carga histórica + cargas diárias anteriores). O volume é a fonte da verdade, então não há tabela de controle para ficar dessincronizada.
# MAGIC - A busca começa no dia seguinte à última data gravada e somente registros com data posterior a ela (e até hoje) são mantidos. Isso é necessário porque a API devolve registros fora do intervalo pedido (séries mensais retornam o mês anterior a `dataInicial`, e a Selic meta já publica datas futuras).
# MAGIC - Cada execução grava um arquivo novo por série (`<serie>_diario_<timestamp>.json`), apenas se houver registros novos. Arquivos nunca são sobrescritos, então o Auto Loader ingere cada um exatamente uma vez.
# MAGIC
# MAGIC ## Execução sem acompanhamento
# MAGIC
# MAGIC - Se um dia falhar (API fora do ar, dado ainda não publicado), a próxima execução recupera o período automaticamente, pois parte sempre da última data gravada.
# MAGIC - Se alguma série falhar, as demais são gravadas normalmente e o notebook termina com erro, para que o Job fique como falho e envie a notificação configurada.
# MAGIC
# MAGIC **Pré-requisito:** a carga histórica (`ingestao_bacen.py`) já ter sido executada.

# COMMAND ----------

# DBTITLE 1,Configuration
import requests
import json
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pyspark.sql.functions import col, to_date, regexp_extract, max as spark_max

VOLUME_PATH = "/Volumes/projeto_hands_on/bronze/raw"
HEADERS = {"User-Agent": "Mozilla/5.0 (Databricks; BACEN data ingestion)"}

# Prefixo do arquivo -> código SGS. Os prefixos precisam bater com os pathGlobFilter do pipeline (ex.: "dolar_*.json")
SERIES = {
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

agora = datetime.now(ZoneInfo("America/Sao_Paulo"))
hoje = agora.date()
print(f"Execução em {agora.strftime('%d/%m/%Y %H:%M:%S')} (America/Sao_Paulo)")

# COMMAND ----------

# DBTITLE 1,Read Last Ingested Date Per Series
arquivos_json = [f for f in dbutils.fs.ls(VOLUME_PATH) if f.name.endswith(".json")]
if not arquivos_json:
    raise RuntimeError(f"Nenhum arquivo encontrado em {VOLUME_PATH}. Execute a carga histórica (ingestao_bacen.py) primeiro.")

# Alternância com "peso_argentino" antes dos demais para o regex não ficar ambíguo
padrao_prefixo = "^(" + "|".join(sorted(SERIES, key=len, reverse=True)) + ")_"

ultimas_datas = {
    row["serie"]: row["ultima_data"]
    for row in (
        spark.read.format("json")
        .load(f"{VOLUME_PATH}/*.json")
        .select(
            regexp_extract(col("_metadata.file_name"), padrao_prefixo, 1).alias("serie"),
            to_date(col("data"), "dd/MM/yyyy").alias("data_ref"),
        )
        .filter((col("serie") != "") & col("data_ref").isNotNull())
        .groupBy("serie")
        .agg(spark_max("data_ref").alias("ultima_data"))
        .collect()
    )
}

for serie in SERIES:
    print(f"  {serie}: {ultimas_datas.get(serie)}")

# COMMAND ----------

# DBTITLE 1,Ingest New Records
def gerar_janelas_10_anos(inicio, fim):
    """Divide um intervalo de datas (date) em janelas de no máximo 10 anos."""
    janelas = []
    atual = inicio
    while atual <= fim:
        try:
            proximo = atual.replace(year=atual.year + 10)
        except ValueError:
            proximo = atual.replace(year=atual.year + 10, day=28)
        proximo = min(proximo, fim)
        janelas.append((atual, proximo))
        atual = proximo + timedelta(days=1)
    return janelas


def buscar_janela(code, inicio, fim):
    """Busca uma janela na API. Retorna a lista de registros ([] se não houver dados) ou None em caso de falha."""
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"
    params = {"formato": "json", "dataInicial": inicio.strftime("%d/%m/%Y"), "dataFinal": fim.strftime("%d/%m/%Y")}
    for attempt in range(3):
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=60)
            if response.status_code == 404:
                return []
            if response.status_code == 200:
                dados = response.json()
                if isinstance(dados, list):
                    return dados
                # A API às vezes responde "sem dados" com HTTP 200 e corpo {"erro": {"statusCode": 404, ...}}
                if isinstance(dados, dict) and dados.get("erro", {}).get("statusCode") == 404:
                    return []
                print(f"resposta inesperada: {str(dados)[:200]}", end=" ")
            else:
                print(f"HTTP {response.status_code}", end=" ")
        except (requests.exceptions.RequestException, json.JSONDecodeError, ValueError):
            print("erro de conexão", end=" ")
        if attempt < 2:
            print(f"tentando ({attempt+1}/3)...", end=" ")
            time.sleep(5 * (attempt + 1))
    return None


def filtrar_novos(registros, ultima_data, hoje):
    """Mantém só registros posteriores a ultima_data e até hoje, sem datas duplicadas."""
    vistos = set()
    filtrados = []
    for record in registros:
        data_ref = datetime.strptime(record["data"], "%d/%m/%Y").date()
        if ultima_data < data_ref <= hoje and record["data"] not in vistos:
            vistos.add(record["data"])
            filtrados.append(record)
    return filtrados


summary = {}
falhas = []

for serie, code in SERIES.items():
    ultima_data = ultimas_datas.get(serie)
    if ultima_data is None:
        print(f"\n{serie}: nenhum dado no volume, execute a carga histórica para esta série")
        falhas.append(serie)
        continue

    inicio = ultima_data + timedelta(days=1)
    if inicio > hoje:
        print(f"\n{serie}: já atualizada até {ultima_data}")
        summary[serie] = 0
        continue

    print(f"\nBuscando {serie} (código {code}, de {inicio} até {hoje})...")
    novos = []
    falhou = False
    for janela_inicio, janela_fim in gerar_janelas_10_anos(inicio, hoje):
        print(f"  Janela {janela_inicio} a {janela_fim}...", end=" ")
        dados = buscar_janela(code, janela_inicio, janela_fim)
        if dados is None:
            # Para na primeira janela com falha para não deixar buracos: a próxima execução continua daqui
            print("falha")
            falhou = True
            break
        print(f"{len(dados)} registros retornados")
        novos.extend(dados)
        time.sleep(2)

    # Mantém só o que é posterior à última data gravada e não é futuro, sem duplicatas
    filtrados = filtrar_novos(novos, ultima_data, hoje)

    if filtrados:
        ndjson_content = "\n".join(json.dumps(record) for record in filtrados)
        file_path = f"{VOLUME_PATH}/{serie}_diario_{agora.strftime('%Y%m%d_%H%M%S')}.json"
        dbutils.fs.put(file_path, ndjson_content, overwrite=False)
        print(f"  -> {len(filtrados)} registros novos gravados em {file_path}")
    else:
        print("  -> nenhum registro novo")

    summary[serie] = len(filtrados)
    if falhou:
        falhas.append(serie)

print("\n=== Resumo da Ingestão Diária ===")
for serie, count in summary.items():
    print(f"  {serie}: {count} registros novos")
print(f"  Total: {sum(summary.values())} registros novos")

if falhas:
    raise RuntimeError(
        f"Falha nas séries: {', '.join(falhas)}. As demais foram gravadas; "
        "a próxima execução recupera o período pendente automaticamente."
    )
