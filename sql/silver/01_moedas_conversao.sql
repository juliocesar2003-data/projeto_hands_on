-- Consolida as cotacoes de cambio em uma unica tabela limpa e tipada
-- Substitua {{PROJECT_ID}} pelo seu projeto GCP

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.cambio_silver.moedas_conversao` AS
SELECT
  'EUR' AS moeda,
  PARSE_DATE('%d/%m/%Y', data) AS data_cotacao,
  SAFE_CAST(valor AS NUMERIC) AS valor_brl
FROM `{{PROJECT_ID}}.cambio_bronze.ext_euro`
WHERE data IS NOT NULL AND valor IS NOT NULL

UNION ALL

SELECT
  'USD' AS moeda,
  PARSE_DATE('%d/%m/%Y', data) AS data_cotacao,
  SAFE_CAST(valor AS NUMERIC) AS valor_brl
FROM `{{PROJECT_ID}}.cambio_bronze.ext_dolar`
WHERE data IS NOT NULL AND valor IS NOT NULL

UNION ALL

SELECT
  'GBP' AS moeda,
  PARSE_DATE('%d/%m/%Y', data) AS data_cotacao,
  SAFE_CAST(valor AS NUMERIC) AS valor_brl
FROM `{{PROJECT_ID}}.cambio_bronze.ext_libra`
WHERE data IS NOT NULL AND valor IS NOT NULL

UNION ALL

SELECT
  'JPY' AS moeda,
  PARSE_DATE('%d/%m/%Y', data) AS data_cotacao,
  SAFE_CAST(valor AS NUMERIC) AS valor_brl
FROM `{{PROJECT_ID}}.cambio_bronze.ext_iene`
WHERE data IS NOT NULL AND valor IS NOT NULL

UNION ALL

SELECT
  'ARS' AS moeda,
  PARSE_DATE('%d/%m/%Y', data) AS data_cotacao,
  SAFE_CAST(valor AS NUMERIC) AS valor_brl
FROM `{{PROJECT_ID}}.cambio_bronze.ext_peso_argentino`
WHERE data IS NOT NULL AND valor IS NOT NULL;
