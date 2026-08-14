-- Tipagem, cast de ids para STRING, valores monetarios para NUMERIC
-- e geracao de uma data de venda simulada (fonte nao possui data real)
-- Substitua {{PROJECT_ID}} pelo seu projeto GCP

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.cambio_silver.vendas` AS
SELECT
  CAST(id AS STRING) AS id,
  CAST(total AS NUMERIC) AS total,
  'USD' AS currency,
  CAST(discountedTotal AS NUMERIC) AS discountedTotal,
  totalProducts,
  totalQuantity,
  DATE_ADD(DATE '2023-01-01', INTERVAL CAST(RAND() * 1095 AS INT64) DAY) AS sales_date,
  ARRAY(
    SELECT AS STRUCT
      CAST(p.id AS STRING) AS product_id,
      p.title AS product_title,
      CAST(p.price AS NUMERIC) AS product_price_usd,
      p.quantity,
      CAST(p.total AS NUMERIC) AS product_total,
      CAST(p.discountPercentage AS NUMERIC) AS product_discountPercentage,
      CAST(p.discountedTotal AS NUMERIC) AS product_discountedTotal
    FROM UNNEST(products) AS p
  ) AS products
FROM `{{PROJECT_ID}}.cambio_bronze.vendas_raw`;
