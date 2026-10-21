# Pipeline de Câmbio e Indicadores Econômicos (BACEN)

Pipeline de dados em camadas (bronze / silver) que ingere cotações de câmbio e indicadores econômicos da API do Banco Central do Brasil, no período de 2016 a 2025. Possui duas implementações: GCP (Cloud Run + BigQuery, original) e Databricks (Unity Catalog + Spark Declarative Pipeline, atual — ver `databricks/README.md`).

## Arquitetura (GCP)

```
API BACEN
        │
        ▼
  Cloud Run Functions  ──►  GCS (bronze, NDJSON)
        │
        ▼
  BigQuery External Tables (bronze)
        │
        ▼
  BigQuery Tables (silver — tipagem, cast, limpeza)
```

## Estrutura do repositório

```
databricks/              # Versão Databricks (atual)
  notebooks/              # Ingestão BACEN + explorador de séries
  transformations/        # Fontes do Spark Declarative Pipeline
    bronze/               # Streaming tables (Auto Loader)
    silver/               # Materialized views
cloud_run/
  ingestao-api-bacen/     # Coleta cotações de câmbio (GCP, original)
sql/
  bronze/                 # External tables sobre os arquivos NDJSON no GCS
  silver/                 # Tabelas limpas, tipadas
```

## Cloud Run Function

### `ingestao-api-bacen`
Busca as séries históricas de câmbio (2016–2025) na API do BACEN para 5 moedas e grava um arquivo NDJSON por moeda em `gs://<bucket>/bronze/<moeda>_2016_2025.json`.

> A função grava em formato **NDJSON** (um objeto JSON por linha), exigido pelo leitor de JSON do BigQuery — arrays JSON tradicionais (`[{...}, {...}]`) não são aceitos.

## Camadas SQL (BigQuery)

| Camada | Arquivo | Descrição |
|---|---|---|
| Bronze | `01_ext_moedas.sql` | External tables sobre os 5 arquivos de câmbio |
| Silver | `01_moedas_conversao.sql` | Consolida e tipa as cotações (`NUMERIC`, `DATE`) |

## Configuração

Este repositório não contém nomes reais de projeto/bucket — substitua os placeholders abaixo antes de usar:

| Placeholder | Onde aparece | Como definir |
|---|---|---|
| `{{PROJECT_ID}}` | arquivos `.sql` | substituir diretamente no texto antes de rodar |
| `{{BUCKET_NAME}}` | arquivos `.sql` | substituir diretamente no texto antes de rodar |
| `BUCKET_NAME` (env var) | `cloud_run/*/main.py` | definir como variável de ambiente no deploy da Cloud Run Function (`--set-env-vars BUCKET_NAME=seu-bucket`) |

## Como rodar

1. Criar um bucket GCS próprio (nome globalmente único) com a estrutura `bronze/`.
2. Deploy da Cloud Run Function `ingestao-api-bacen`, definindo a env var `BUCKET_NAME` apontando para o bucket criado.
3. Rodar os scripts em `sql/bronze/` (substituindo `{{PROJECT_ID}}` e `{{BUCKET_NAME}}` pelos seus valores).
4. Rodar os scripts em `sql/silver/` (substituindo `{{PROJECT_ID}}`).

## Observações

- Nomes de bucket e dataset devem ser únicos/ajustados ao seu projeto antes de rodar os scripts.
- No Databricks, a ingestão cobre também os indicadores econômicos (Selic 432, CDI 12, TR 226, IPCA 433) além das 5 moedas.
