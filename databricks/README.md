# Databricks Version — projeto_hands_on

This is the Databricks port of the original GCP pipeline (see `cloud_run/` and `sql/` at the repo root for the original version).

## Architecture

```
BACEN API (9 SGS series)
      │
      ▼
Ingestion Notebook ──── triggered by ──→ On-Demand Job (serverless, manual trigger)
      │
      ▼
UC Volume: projeto_hands_on.bronze.raw (9 NDJSON files)
      │
      ▼
Spark Declarative Pipeline (serverless)
      │
      ├─ Bronze: streaming tables via Auto Loader
      │    bronze_moedas (5 currency append flows)
      │    bronze_selic, bronze_cdi, bronze_tr, bronze_ipca
      │
      └─ Silver: materialized views
           silver.moedas_conversao — typed currencies with ISO codes
           silver.selic — monthly (group-by month, MAX, first day)
           silver.cdi — monthly (group-by month, MAX, first day)
           silver.tr — daily with data_ref + data_fim
           silver.ipca — monthly % variation
```

## Contents

```
databricks/
├── notebooks/
│   ├── ingestao_bacen.py            # Ingestion notebook (BACEN API → NDJSON in UC Volume)
│   └── bacen_series_explorer.py     # Exploratory notebook (last 10 records per SGS series)
└── transformations/
    ├── bronze/
    │   ├── bronze_moedas.py         # Currency streaming table (5 append flows)
    │   └── bronze_indicadores.py    # Selic/CDI/TR/IPCA streaming tables
    └── silver/
        ├── silver_moedas_conversao.py  # Typed currency rates
        └── silver_indicadores.py       # Monthly indicators (Selic, CDI), daily TR, monthly IPCA
```

## Setup

1. Run `notebooks/ingestao_bacen.py` (or trigger the on-demand Job) to create the catalog/schema/volume and fetch raw data.
2. Create a Spark Declarative Pipeline in the Databricks UI pointing at `transformations/` as the source, with catalog `projeto_hands_on`, schema `bronze`, serverless compute.
3. Run a pipeline update to materialize bronze and silver tables.

## BACEN SGS Series Used

| Series | Code | Frequency |
|---|---|---|
| EUR/BRL | 21619 | Daily |
| USD/BRL | 10813 | Daily |
| GBP/BRL | 21623 | Daily |
| JPY/BRL | 21621 | Daily |
| ARS/BRL | 21632 | Daily |
| Selic target | 432 | Daily (grouped monthly) |
| CDI | 12 | Daily (grouped monthly) |
| TR | 226 | Daily |
| IPCA % | 433 | Monthly |
