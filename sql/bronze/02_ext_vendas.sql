-- External table apontando para o bucket bronze (dados brutos do dummyjson)
-- Substitua {{PROJECT_ID}} pelo seu projeto GCP

CREATE OR REPLACE EXTERNAL TABLE `{{PROJECT_ID}}.cambio_bronze.vendas_raw`
OPTIONS (
  format = 'JSON',
  uris = ['gs://{{BUCKET_NAME}}/bronze/vendas_usd.json']
);
