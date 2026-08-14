# Pipeline de Câmbio e Vendas (BACEN + DummyJSON → GCS → BigQuery)

Pipeline de dados em camadas (bronze / silver / gold) que ingere cotações de câmbio da API do Banco Central do Brasil e dados de vendas simulados (DummyJSON), armazena em Google Cloud Storage e transforma no BigQuery até uma tabela final de vendas convertidas para BRL.

## Arquitetura

```
APIs (BACEN / DummyJSON)
        │
        ▼
  Cloud Run Functions  ──►  GCS (bronze, NDJSON)
        │
        ▼
  BigQuery External Tables (bronze)
        │
        ▼
  BigQuery Tables (silver — tipagem, cast, limpeza)
        │
        ▼
  BigQuery Tables (gold — conversão USD → BRL)
```

## Estrutura do repositório

```
cloud_run/
  ingestao-api-bacen/     # Coleta cotações de câmbio (EUR, USD, GBP, JPY, ARS)
  ingestao-api-dummy/     # Coleta dados de vendas (dummyjson.com/carts)
sql/
  bronze/                 # External tables sobre os arquivos NDJSON no GCS
  silver/                 # Tabelas limpas, tipadas
  gold/                   # Tabela final de vendas convertida para BRL
```

## Cloud Run Functions

### `ingestao-api-bacen`
Busca as séries históricas de câmbio (2023–2025) na API do BACEN para 5 moedas e grava um arquivo NDJSON por moeda em `gs://<bucket>/bronze/<moeda>_2023_2025.json`.

### `ingestao-api-dummy`
Busca os carrinhos de venda da API pública [dummyjson.com](https://dummyjson.com/carts) e grava um único NDJSON em `gs://<bucket>/bronze/vendas_usd.json`.

> Ambas as funções gravam em formato **NDJSON** (um objeto JSON por linha), exigido pelo leitor de JSON do BigQuery — arrays JSON tradicionais (`[{...}, {...}]`) não são aceitos.

## Camadas SQL (BigQuery)

| Camada | Arquivo | Descrição |
|---|---|---|
| Bronze | `01_ext_moedas.sql` | External tables sobre os 5 arquivos de câmbio |
| Bronze | `02_ext_vendas.sql` | External table sobre o arquivo de vendas |
| Silver | `01_moedas_conversao.sql` | Consolida e tipa as cotações (`NUMERIC`, `DATE`) |
| Silver | `02_vendas.sql` | Tipagem de vendas: ids como `STRING`, valores monetários como `NUMERIC`, geração de `sales_date` simulada (a API de origem não possui data) |
| Gold | `01_vendas.sql` | `UNNEST` dos produtos + join com cotações, gerando valores convertidos para BRL |

## Configuração

Este repositório não contém nomes reais de projeto/bucket — substitua os placeholders abaixo antes de usar:

| Placeholder | Onde aparece | Como definir |
|---|---|---|
| `{{PROJECT_ID}}` | arquivos `.sql` | substituir diretamente no texto antes de rodar |
| `{{BUCKET_NAME}}` | arquivos `.sql` | substituir diretamente no texto antes de rodar |
| `BUCKET_NAME` (env var) | `cloud_run/*/main.py` | definir como variável de ambiente no deploy da Cloud Run Function (`--set-env-vars BUCKET_NAME=seu-bucket`) |

## Como rodar

1. Criar um bucket GCS próprio (nome globalmente único) com a estrutura `bronze/`.
2. Deploy das Cloud Run Functions (`ingestao-api-bacen`, `ingestao-api-dummy`), definindo a env var `BUCKET_NAME` apontando para o bucket criado.
3. Rodar os scripts em `sql/bronze/` (substituindo `{{PROJECT_ID}}` e `{{BUCKET_NAME}}` pelos seus valores).
4. Rodar os scripts em `sql/silver/` (substituindo `{{PROJECT_ID}}`).
5. Rodar os scripts em `sql/gold/` (substituindo `{{PROJECT_ID}}`).

## Observações

- A data de venda (`sales_date`) é **simulada aleatoriamente** entre 2023-01-01 e 2025-12-31, pois a API de vendas usada (DummyJSON) não fornece datas reais — usada apenas para fins de prática de junção com a tabela de câmbio por data.
- Nomes de bucket e dataset devem ser únicos/ajustados ao seu projeto antes de rodar os scripts.
