-- Converte os valores de venda (USD) para BRL usando a cotacao do dia
-- Substitua {{PROJECT_ID}} pelo seu projeto GCP

CREATE OR REPLACE TABLE `{{PROJECT_ID}}.cambio_gold.vendas` AS
SELECT
  v.* EXCEPT(products),
  p.product_id,
  p.product_title,
  p.product_price_usd,
  p.product_price_usd * mc.valor_brl AS product_price_brl,
  p.product_total * mc.valor_brl AS product_total_brl,
  p.product_discountedTotal * mc.valor_brl AS product_discountedTotal_brl,
  v.total * mc.valor_brl AS total_brl
FROM `{{PROJECT_ID}}.cambio_silver.vendas` AS v,
UNNEST(v.products) AS p
LEFT JOIN `{{PROJECT_ID}}.cambio_silver.moedas_conversao` mc
  ON v.currency = mc.moeda
  AND v.sales_date = mc.data_cotacao;
