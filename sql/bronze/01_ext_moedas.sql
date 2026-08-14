-- External tables apontando para o bucket bronze (dados brutos do BACEN)
-- Substitua {{PROJECT_ID}} pelo seu projeto GCP

CREATE OR REPLACE EXTERNAL TABLE `{{PROJECT_ID}}.cambio_bronze.ext_euro`
(data STRING, valor STRING)
OPTIONS (format = 'JSON', uris = ['gs://{{BUCKET_NAME}}/bronze/euro_2023_2025.json']);

CREATE OR REPLACE EXTERNAL TABLE `{{PROJECT_ID}}.cambio_bronze.ext_dolar`
(data STRING, valor STRING)
OPTIONS (format = 'JSON', uris = ['gs://{{BUCKET_NAME}}/bronze/dolar_2023_2025.json']);

CREATE OR REPLACE EXTERNAL TABLE `{{PROJECT_ID}}.cambio_bronze.ext_libra`
(data STRING, valor STRING)
OPTIONS (format = 'JSON', uris = ['gs://{{BUCKET_NAME}}/bronze/libra_2023_2025.json']);

CREATE OR REPLACE EXTERNAL TABLE `{{PROJECT_ID}}.cambio_bronze.ext_iene`
(data STRING, valor STRING)
OPTIONS (format = 'JSON', uris = ['gs://{{BUCKET_NAME}}/bronze/iene_2023_2025.json']);

CREATE OR REPLACE EXTERNAL TABLE `{{PROJECT_ID}}.cambio_bronze.ext_peso_argentino`
(data STRING, valor STRING)
OPTIONS (format = 'JSON', uris = ['gs://{{BUCKET_NAME}}/bronze/peso_argentino_2023_2025.json']);
