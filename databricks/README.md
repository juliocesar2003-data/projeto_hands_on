# Pipeline de Câmbio e Indicadores Econômicos (BACEN) — Databricks

Pipeline de dados em camadas (bronze / silver) no Databricks que ingere cotações de câmbio e indicadores econômicos da API do Banco Central do Brasil, cobrindo todo o histórico disponível desde a data mais antiga de cada série (a partir de 1980) até 31/12/2025. A API do BACEN permite no máximo 10 anos por consulta, então os dados são extraídos em janelas de 10 anos.

## Arquitetura

```
BACEN API (9 séries SGS)
      │
      ▼
Notebook de Ingestão ──── disparado por ──→ Job sob demanda (serverless)
      │
      ▼
UC Volume: projeto_hands_on.bronze.raw (9 arquivos NDJSON)
      │
      ▼
Spark Declarative Pipeline (serverless)
      │
      ├─ Bronze: streaming tables via Auto Loader
      │    bronze_moedas (5 fluxos append, um por moeda)
      │    bronze_selic, bronze_cdi, bronze_tr, bronze_ipca
      │
      └─ Silver: materialized views
           silver.moedas_conversao — moedas tipadas com códigos ISO
           silver.selic — mensal (agrupamento por mês, MAX, primeiro dia)
           silver.cdi — mensal (agrupamento por mês, MAX, primeiro dia)
           silver.tr — diário com data_ref + data_fim
           silver.ipca — variação percentual mensal
```

## Conteúdo

```
notebooks/
├── ingestao_bacen.py            # Notebook de ingestão (BACEN API → NDJSON no UC Volume)
└── bacen_series_explorer.py     # Notebook exploratório (últimos 10 registros por série SGS)
transformations/
├── bronze/
│   ├── bronze_moedas.py         # Streaming table de câmbio (5 fluxos append)
│   └── bronze_indicadores.py    # Streaming tables Selic/CDI/TR/IPCA
└── silver/
    ├── silver_moedas_conversao.py  # Cotações de moedas tipadas
    └── silver_indicadores.py       # Indicadores mensais (Selic, CDI), diários (TR), mensais (IPCA)
```

## Configuração

1. Executar `notebooks/ingestao_bacen.py` (ou disparar o Job sob demanda) para criar o catálogo `projeto_hands_on`, o schema `bronze` e o volume `raw`, e buscar todo o histórico disponível da API do BACEN em janelas de 10 anos.
2. Criar um Spark Declarative Pipeline no Databricks apontando para o diretório `transformations/` como origem, com catálogo `projeto_hands_on`, schema `bronze` e compute serverless.
3. Executar uma atualização do pipeline para materializar as tabelas bronze e silver.

## Séries do BACEN SGS utilizadas

| Série | Código | Frequência |
|---|---|---|
| EUR/BRL | 21619 | Diária |
| USD/BRL | 1 | Diária |
| GBP/BRL | 21623 | Diária |
| JPY/BRL | 21621 | Diária |
| ARS/BRL | 21632 | Diária |
| Selic (meta Copom) | 432 | Diária (agrupada mensal) |
| CDI | 12 | Diária (agrupada mensal) |
| TR | 226 | Diária |
| IPCA % | 433 | Mensal |
