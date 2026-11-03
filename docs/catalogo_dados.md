# Catálogo e Dicionário de Dados

## Projeto Hands-on BACEN

- **Versão da documentação:** 1.2
- **Data de referência:** 03/11/2026
- **Catálogo no Unity Catalog:** `projeto_hands_on`
- **Responsável:** equipe mantenedora do Projeto Hands-on BACEN
- **Status:** vigente

---

## 1. Objetivo

Este documento descreve os dados produzidos pelo pipeline de câmbio e indicadores econômicos do Banco Central do Brasil. O catálogo foi elaborado para facilitar a consulta acadêmica, técnica e funcional sem exigir acesso direto ao workspace do Databricks.

O documento reúne:

- origem e finalidade dos dados;
- organização das camadas Bronze, Silver e Gold;
- granularidade e chaves das tabelas;
- descrição, tipo e unidade das colunas;
- regras de transformação;
- regras de qualidade;
- frequência de atualização;
- produtos consumidores;
- glossário e limitações.

> As análises e produtos derivados possuem finalidade descritiva e acadêmica. Os resultados não constituem previsão ou recomendação financeira.

---

## 2. Arquitetura de dados

```text
API SGS do BACEN
        ↓
Carga histórica + ingestão diária incremental
        ↓
Unity Catalog Volume / arquivos NDJSON
        ↓
Bronze: dados brutos e linhagem
        ↓
Silver: tipagem, deduplicação, agregação e qualidade
        ↓
Gold: features analíticas e tabelas orientadas ao consumo
        ↓
EDA no Databricks / Power BI / modelos analíticos
```

### 2.1 Identificação das camadas

| Camada | Finalidade | Principais consumidores |
|---|---|---|
| Bronze | Preservar os dados próximos à origem e registrar sua linhagem | Transformações Silver, auditoria e reprocessamento |
| Silver | Limpar, tipar, deduplicar, agregar e validar as séries | EDA, monitoramento e tabelas Gold |
| Gold | Disponibilizar dados orientados a produtos analíticos | Modelos preditivos e dashboards de negócio |

---

## 3. Origem dos dados

Os dados são obtidos da API do **Sistema Gerenciador de Séries Temporais (SGS)** do Banco Central do Brasil.

| Série | Código SGS | Frequência de origem | Data histórica inicial aproximada |
|---|---:|---|---|
| USD/BRL | 1 | Diária | 28/11/1984 |
| CDI | 12 | Diária | 06/03/1986 |
| TR | 226 | Diária | 01/02/1991 |
| Selic meta | 432 | Diária | 05/03/1999 |
| IPCA | 433 | Mensal | 01/01/1980 |
| EUR/BRL | 21619 | Diária | 31/12/1998 |
| JPY/BRL | 21621 | Diária | 01/07/1994 |
| GBP/BRL | 21623 | Diária | 01/07/1994 |
| ARS/BRL | 21632 | Diária | 01/07/1994 |

### 3.1 Estratégia de ingestão

- A carga histórica consulta a API em janelas de no máximo dez anos.
- A ingestão diária parte da última data gravada para cada série.
- Datas futuras e observações já armazenadas são descartadas.
- Cada execução incremental grava um novo arquivo NDJSON no volume.
- Os arquivos não são sobrescritos durante a ingestão diária.
- O Auto Loader identifica os novos arquivos e alimenta as tabelas Bronze.

---

# 4. Camada Bronze

## 4.1 `projeto_hands_on.bronze.bronze_moedas`

### Identificação

- **Finalidade:** armazenar as cotações brutas das cinco moedas.
- **Origem:** arquivos NDJSON no Unity Catalog Volume.
- **Granularidade:** uma linha por moeda e data retornada pela API.
- **Chave lógica:** `moeda_arquivo` + `data`.
- **Tipo de objeto:** streaming table.
- **Atualização:** ingestão incremental diária via Auto Loader.

### Colunas

| Coluna | Tipo | Unidade | Nulo | Descrição |
|---|---|---|---|---|
| `data` | String | Data no formato `dd/MM/yyyy` | Pode ocorrer na Bronze | Data recebida da API |
| `valor` | String | Cotação em BRL | Pode ocorrer na Bronze | Valor bruto recebido da API |
| `moeda_arquivo` | String | Código técnico | Não esperado | Identifica `euro`, `dolar`, `libra`, `iene` ou `peso_argentino` |
| `origem_ingestao` | String | Caminho | Não esperado | Arquivo do volume que originou o registro |
| `data_ingestao` | Timestamp | UTC | Não esperado | Data de gravação do arquivo no volume |
| `data_processamento` | Timestamp | UTC | Não esperado | Data em que o pipeline leu o registro |

### Regras de qualidade

| Regra | Severidade | Condição |
|---|---|---|
| `data_valida` | Registrar | A data deve ser convertível no formato `dd/MM/yyyy` |
| `valor_numerico` | Registrar | O valor deve ser convertível para número |
| `cotacao_positiva` | Registrar | A cotação deve ser maior que zero |
| `origem_ingestao_preenchida` | Registrar | O caminho de origem deve estar preenchido |

---

## 4.2 `projeto_hands_on.bronze.bronze_selic`

- **Finalidade:** armazenar os registros brutos da meta Selic.
- **Código SGS:** 432.
- **Granularidade:** uma linha por data retornada pela API.
- **Chave lógica:** `data`.
- **Tipo de objeto:** streaming table.

### Colunas comuns aos indicadores Bronze

| Coluna | Tipo | Unidade | Descrição |
|---|---|---|---|
| `data` | String | `dd/MM/yyyy` | Data bruta da observação |
| `valor` | String | Conforme a série | Valor bruto da API |
| `origem_ingestao` | String | Caminho | Arquivo de origem |
| `data_ingestao` | Timestamp | UTC | Data de gravação no volume |
| `data_processamento` | Timestamp | UTC | Data de leitura pelo pipeline |

### Regras de qualidade

- `data_valida`;
- `valor_numerico`;
- `origem_ingestao_preenchida`.

---

## 4.3 `projeto_hands_on.bronze.bronze_cdi`

- **Finalidade:** armazenar os registros brutos da taxa CDI diária.
- **Código SGS:** 12.
- **Granularidade:** uma linha por dia útil com observação.
- **Chave lógica:** `data`.
- **Tipo de objeto:** streaming table.
- **Colunas e regras:** iguais às colunas comuns dos indicadores Bronze.

---

## 4.4 `projeto_hands_on.bronze.bronze_tr`

- **Finalidade:** armazenar os registros brutos da Taxa Referencial.
- **Código SGS:** 226.
- **Granularidade:** uma linha por período de vigência da TR.
- **Chave lógica:** `data` + `dataFim`.
- **Tipo de objeto:** streaming table.

### Coluna adicional

| Coluna | Tipo | Unidade | Descrição |
|---|---|---|---|
| `dataFim` | String | `dd/MM/yyyy` | Data final do período da TR |

### Regra adicional

| Regra | Severidade | Condição |
|---|---|---|
| `periodo_valido` | Registrar | `dataFim` deve ser posterior a `data` |

---

## 4.5 `projeto_hands_on.bronze.bronze_ipca`

- **Finalidade:** armazenar os registros brutos da variação mensal do IPCA.
- **Código SGS:** 433.
- **Granularidade:** uma linha por mês retornado pela API.
- **Chave lógica:** `data`.
- **Tipo de objeto:** streaming table.
- **Colunas e regras:** iguais às colunas comuns dos indicadores Bronze.

---

# 5. Camada Silver

## 5.1 `projeto_hands_on.silver.moedas_conversao`

### Identificação

- **Finalidade:** consolidar e tipar as cotações de EUR, USD, GBP, JPY e ARS.
- **Origem:** `bronze_moedas`.
- **Granularidade:** uma linha por moeda e data de cotação.
- **Chave:** `moeda` + `data_cotacao`.
- **Tipo de objeto:** materialized view.
- **Consumidores:** notebook de EDA, dashboard exploratório e tabelas Gold.

### Mapeamento de moedas

| Valor Bronze | Código Silver |
|---|---|
| `euro` | `EUR` |
| `dolar` | `USD` |
| `libra` | `GBP` |
| `iene` | `JPY` |
| `peso_argentino` | `ARS` |

### Colunas

| Coluna | Tipo | Unidade | Nulo | Descrição |
|---|---|---|---|---|
| `moeda` | String | ISO 4217 | Não | Código padronizado da moeda |
| `data_cotacao` | Date | Data | Não | Data da cotação |
| `valor_brl` | Decimal(18,6) | BRL por unidade da moeda | Não | Cotação convertida para tipo numérico |

### Regras de qualidade

| Regra | Severidade | Condição |
|---|---|---|
| `moeda_conhecida` | Interromper | A moeda deve estar presente no mapeamento |
| `data_cotacao_valida` | Interromper | A data deve ser válida |
| `cotacao_positiva` | Descartar | A cotação deve ser maior que zero |

---

## 5.2 `projeto_hands_on.silver.selic`

### Identificação

- **Finalidade:** disponibilizar a meta Selic mensal em unidade comparável ao CDI anual.
- **Origem:** `bronze_selic`.
- **Granularidade:** uma linha por mês.
- **Chave:** `data_ref`.
- **Código SGS:** 432.
- **Tipo de objeto:** materialized view.

### Regra de transformação

A Selic mensal corresponde ao valor vigente na última data disponível do mês. A regra usa o último valor por data, e não o máximo mensal, evitando distorções em meses com decisões do Copom.

### Colunas

| Coluna | Tipo | Unidade | Nulo | Descrição |
|---|---|---|---|---|
| `data_ref` | Date | Mês de referência | Não | Primeiro dia do mês representado |
| `valor` | Decimal(18,6) | % ao ano | Não | Meta Selic vigente no encerramento do mês |

### Regras de qualidade

| Regra | Severidade | Condição |
|---|---|---|
| `data_ref_valida` | Interromper | A data de referência deve estar preenchida |
| `faixa_plausivel` | Registrar | Valor maior que zero e menor ou igual a 100% a.a. |

---

## 5.3 `projeto_hands_on.silver.cdi`

### Identificação

- **Finalidade:** disponibilizar o CDI mensal em unidade anual e mensal.
- **Origem:** `bronze_cdi`.
- **Granularidade:** uma linha por mês.
- **Chave:** `data_ref`.
- **Código SGS:** 12.
- **Tipo de objeto:** materialized view.

### Regras de transformação

A taxa diária é anualizada considerando 252 dias úteis:

```text
Taxa anual equivalente = ((1 + taxa_dia / 100) ^ 252 - 1) × 100
```

A taxa mensal é calculada pela composição das taxas diárias:

```text
Acumulado do mês = (produto(1 + taxa_dia / 100) - 1) × 100
```

### Colunas

| Coluna | Tipo | Unidade | Nulo | Descrição |
|---|---|---|---|---|
| `data_ref` | Date | Mês de referência | Não | Primeiro dia do mês representado |
| `valor` | Decimal(18,6) | % ao ano | Não | Média mensal da taxa diária anualizada |
| `acumulado_mes` | Decimal(18,6) | % no mês | Não | Composição das taxas diárias do mês |
| `dias_uteis` | Integer | Dias | Não | Quantidade de observações utilizadas no mês |

### Regras de qualidade

| Regra | Severidade | Condição |
|---|---|---|
| `data_ref_valida` | Interromper | A data de referência deve estar preenchida |
| `taxas_nao_negativas` | Registrar | `valor` e `acumulado_mes` devem ser não negativos |
| `dias_uteis_no_mes` | Registrar | Quantidade entre 1 e 23 dias |

---

## 5.4 `projeto_hands_on.silver.tr`

### Identificação

- **Finalidade:** disponibilizar a Taxa Referencial diária com período de vigência.
- **Origem:** `bronze_tr`.
- **Granularidade:** uma linha por `data_ref` e `data_fim`.
- **Chave:** `data_ref` + `data_fim`.
- **Código SGS:** 226.
- **Tipo de objeto:** materialized view.

### Colunas

| Coluna | Tipo | Unidade | Nulo | Descrição |
|---|---|---|---|---|
| `data_ref` | Date | Data | Não | Início do período da TR |
| `data_fim` | Date | Data | Não | Fim do período da TR |
| `valor` | Decimal(18,6) | Taxa no período | Não | Valor da Taxa Referencial |

### Regras de qualidade

| Regra | Severidade | Condição |
|---|---|---|
| `data_ref_valida` | Interromper | A data de referência deve estar preenchida |
| `taxa_nao_negativa` | Registrar | A TR deve ser maior ou igual a zero |
| `periodo_valido` | Registrar | `data_fim` deve ser posterior a `data_ref` |

> Valores iguais a zero são válidos para a TR e não são tratados automaticamente como erro.

---

## 5.5 `projeto_hands_on.silver.ipca`

### Identificação

- **Finalidade:** disponibilizar a variação mensal do IPCA sem duplicidades.
- **Origem:** `bronze_ipca`.
- **Granularidade:** uma linha por mês.
- **Chave:** `data_ref`.
- **Código SGS:** 433.
- **Tipo de objeto:** materialized view.

### Regra de transformação

A tabela remove duplicidades por `data_ref`. Essa deduplicação trata repetições geradas pela resposta da API nos limites das janelas históricas de dez anos.

### Colunas

| Coluna | Tipo | Unidade | Nulo | Descrição |
|---|---|---|---|---|
| `data_ref` | Date | Mês de referência | Não | Primeiro dia do mês representado |
| `valor` | Decimal(18,6) | % no mês | Não | Variação mensal do IPCA |

### Regras de qualidade

| Regra | Severidade | Condição |
|---|---|---|
| `data_ref_valida` | Interromper | A data de referência deve estar preenchida |
| `faixa_plausivel` | Registrar | Valor entre -5% e 100% no mês |

---

## 5.6 `projeto_hands_on.silver.dq_resumo`

### Identificação

- **Finalidade:** monitorar unicidade, volume e atualização das tabelas Silver.
- **Origem:** tabelas Silver.
- **Granularidade:** uma linha por tabela monitorada.
- **Chave:** `tabela`.
- **Tipo de objeto:** materialized view.
- **Consumidores:** operação do pipeline, página de qualidade do Power BI e auditoria.

### Tabelas monitoradas

- `moedas_conversao`;
- `selic`;
- `cdi`;
- `tr`;
- `ipca`.

### Colunas

| Coluna | Tipo | Unidade | Descrição |
|---|---|---|---|
| `tabela` | String | Nome técnico | Tabela Silver monitorada |
| `linhas` | Long | Registros | Quantidade total de linhas |
| `chaves_distintas` | Long | Chaves | Quantidade de chaves distintas |
| `ultima_data` | Date | Data | Data mais recente da série |
| `dias_desde_ultima_data` | Integer | Dias | Diferença entre a data atual e a última observação |
| `limite_atraso_dias` | Integer | Dias | Atraso máximo aceitável para a série |
| `verificado_em` | Timestamp | Data/hora | Momento da verificação |

### Limites de atraso

| Tabela | Limite |
|---|---:|
| `moedas_conversao` | 7 dias |
| `tr` | 7 dias |
| `selic` | 38 dias |
| `cdi` | 38 dias |
| `ipca` | 75 dias |

### Regras de qualidade

| Regra | Severidade | Condição |
|---|---|---|
| `chave_unica` | Interromper | `linhas` deve ser igual a `chaves_distintas` |
| `dados_atualizados` | Registrar | O atraso deve ser menor ou igual ao limite |

---

# 6. Camada Gold

## 6.1 `projeto_hands_on.gold.metas_inflacao`

### Identificação

- **Finalidade:** manter as metas de inflação definidas pelo Conselho Monetário Nacional.
- **Granularidade:** uma linha por ano.
- **Chave:** `ano`.
- **Origem:** estrutura de referência mantida em código.
- **Tipo de objeto:** materialized view.
- **Consumidores:** `gold.features_mensal` e análises de inflação.

### Colunas

| Coluna | Tipo | Unidade | Descrição |
|---|---|---|---|
| `ano` | Integer | Ano | Ano de referência da meta |
| `meta` | Double | % | Meta central de inflação |
| `tolerancia_pp` | Double | Pontos percentuais | Tolerância acima e abaixo da meta |
| `limite_inferior` | Double | % | Meta menos tolerância |
| `limite_superior` | Double | % | Meta mais tolerância |
| `norma` | String | Referência | Norma do CMN associada |

### Regras de qualidade

| Regra | Severidade | Condição |
|---|---|---|
| `ano_valido` | Interromper | O ano deve estar preenchido |
| `meta_positiva` | Interromper | Meta e tolerância devem ser positivas |

---

## 6.2 `projeto_hands_on.gold.features_mensal`

### Identificação

- **Finalidade:** fornecer features e alvos para o modelo de previsão mensal do IPCA.
- **Granularidade:** uma linha por mês base com IPCA divulgado.
- **Chave:** `mes_base`.
- **Período de features:** a partir de 01/2001. A Selic meta começa em 03/1999, e a variação da Selic em 12 meses precisa de um ano dela.
- **Separação temporal:** validação ano a ano (origem móvel) de 2005 em diante: cada ano é testado com modelos treinados apenas com dados até o fim do ano anterior.
- **Tipo de objeto:** materialized view.
- **Consumidores:** treinamento, teste, previsão e simuladores analíticos.

> Esta tabela é voltada a machine learning e não deve ser confundida com a Gold específica para dashboards de negócio.

### Grupos de colunas

#### Controle

| Coluna | Tipo esperado | Descrição |
|---|---|---|
| `mes_base` | Date | Último mês conhecido usado como base |
| `mes_h1` | Date | Primeiro mês a ser previsto |
| `mes_calendario` | Integer | Número do mês do calendário |
| `alvos_disponiveis` | Integer | Quantidade de horizontes conhecidos |
| `features_completas` | Boolean | Indica completude das features |
| `conjunto` | String | `historico` (com pelo menos um alvo conhecido) ou `previsao` (mês base mais recente) |
| `data_calculo` | Date/Timestamp | Momento de cálculo |

#### IPCA e sazonalidade

- `ipca_lag1`, `ipca_lag2`, `ipca_lag3`, `ipca_lag12`;
- `ipca_acum_3m`, `ipca_acum_12m`;
- `ipca_sazonal_h1` a `ipca_sazonal_h12`;
- `ipca_menos_sazonal_lag1`.

Unidade predominante: percentual ou pontos percentuais.

#### Meta de inflação

- `meta_inflacao`;
- `ipca_12m_menos_meta`.

#### Selic e juro real

- `selic_lag1`;
- `selic_var_3m_pp`;
- `selic_var_12m_pp`;
- `meses_desde_mudanca_selic`;
- `juro_real_12m`.

#### Dólar

- `usd_lag1`;
- `usd_var_lag1`;
- `usd_var_3m`;
- `usd_var_12m`;
- `usd_volatilidade_lag1`;
- `usd_menos_ipca_12m`.

#### Padronização e comparações

- `usd_var_12m_z`;
- `selic_var_12m_pp_z`;
- `ipca_acum_12m_z`;
- `usd_vs_selic_z`;
- `ipca_vs_selic_z`;
- `usd_vs_ipca_z`.

Média e desvio padrão dos z-scores usam o histórico desde 01/1997 até o próprio mês base, sem informação futura. As médias sazonais (`ipca_sazonal_h*`) usam o mesmo início de histórico.

#### Alvos

- `alvo_ipca_h1` a `alvo_ipca_h12`;
- `alvo_ipca_acum_12m`.

### Regras de qualidade

- mês base obrigatório;
- features completas;
- meta de inflação cadastrada;
- IPCA em faixa plausível;
- Selic em faixa plausível;
- dólar positivo.

---

## 6.3 `projeto_hands_on.gold.bi_economia_mensal` — Planejada

### Status

**Tabela proposta para inclusão no pipeline.** Enquanto não for implantada, o dashboard de EDA continua consumindo diretamente as tabelas Silver.

### Identificação

- **Finalidade:** fornecer uma base econômica mensal simplificada para o dashboard de negócio no Power BI.
- **Granularidade:** uma linha por mês.
- **Chave:** `mes`.
- **Origens:** `silver.selic`, `silver.cdi`, `silver.ipca`, `silver.tr` e `silver.moedas_conversao`.
- **Consumidor:** futuro dashboard executivo no Power BI.

### Colunas propostas

| Coluna | Tipo | Unidade | Origem/regra |
|---|---|---|---|
| `mes` | Date | Mês | Chave mensal |
| `selic_aa` | Double | % a.a. | `silver.selic.valor` |
| `cdi_aa` | Double | % a.a. | `silver.cdi.valor` |
| `cdi_acumulado_mes` | Double | % no mês | `silver.cdi.acumulado_mes` |
| `cdi_dias_uteis` | Integer | Dias | `silver.cdi.dias_uteis` |
| `tr_media_mes` | Double | Taxa do período | Média mensal de `silver.tr.valor` |
| `ipca_mes` | Double | % no mês | `silver.ipca.valor` |
| `usd_fechamento` | Double | BRL por USD | Última cotação disponível no mês |
| `eur_fechamento` | Double | BRL por EUR | Última cotação disponível no mês |
| `gbp_fechamento` | Double | BRL por GBP | Última cotação disponível no mês |
| `jpy_fechamento` | Double | BRL por JPY | Última cotação disponível no mês |
| `ars_fechamento` | Double | BRL por ARS | Última cotação disponível no mês |
| `data_calculo` | Timestamp | Data/hora | Momento de atualização da Gold |

### Regras de qualidade propostas

- `mes` obrigatório e único;
- Selic entre 0 e 100% a.a., quando preenchida;
- CDI não negativo, quando preenchido;
- IPCA entre -5% e 100% no mês, quando preenchido;
- TR não negativa, quando preenchida;
- cotações positivas, quando preenchidas.

---

## 6.4 `projeto_hands_on.gold.coeficientes_modelo`

### Identificação

- **Finalidade:** registrar os coeficientes da regressão Ridge (modelo de comparação) por alvo, em unidades originais.
- **Granularidade:** uma linha por alvo e feature (inclui o intercepto).
- **Chave:** `alvo` + `feature`.
- **Origem:** notebook `treino_modelo_ipca.py`, a partir de `gold.features_mensal`.
- **Tipo de objeto:** tabela Delta recriada a cada treino (não faz parte do pipeline).
- **Atualização:** tarefa `treino_modelo` do Job diário, somente quando há IPCA novo.

### Colunas

| Coluna | Tipo | Unidade | Descrição |
|---|---|---|---|
| `alvo` | String | Código | `h1` a `h12` ou `acum_12m` |
| `horizonte` | Long | Meses | Meses entre o mês base e o mês previsto |
| `feature` | String | Nome técnico | Variável do modelo ou `intercepto` |
| `coluna_origem` | String | Nome técnico | Coluna de `gold.features_mensal` usada pela feature |
| `coeficiente` | Double | Conforme a feature | Efeito na previsão: `previsao = intercepto + soma(coeficiente × feature)` |
| `alpha` | Double | Adimensional | Força da regularização escolhida por validação temporal |
| `n_treino` | Long | Meses | Meses usados no ajuste final |
| `erro_p10` | Double | p.p. | Quantil de 10% dos erros do Ridge nos testes |
| `erro_p90` | Double | p.p. | Quantil de 90% dos erros do Ridge nos testes |
| `run_id` | String | Identificador | Execução correspondente no MLflow |
| `treinado_em` | Timestamp | UTC | Momento do treino |

---

## 6.5 `projeto_hands_on.gold.parametros_arima`

### Identificação

- **Finalidade:** registrar os parâmetros do ARIMA, modelo oficial da previsão, e dos SARIMAX, que acrescentam as variáveis externas juro real e dólar.
- **Granularidade:** uma linha por modelo e parâmetro.
- **Chave:** `modelo` + `parametro`.
- **Origem:** notebook `treino_modelo_ipca.py`, a partir das tabelas Silver de IPCA, Selic e câmbio.
- **Tipo de objeto:** tabela Delta recriada a cada treino (não faz parte do pipeline).

### Colunas

| Coluna | Tipo | Unidade | Descrição |
|---|---|---|---|
| `modelo` | String | Código | `arima`, `sarimax` ou `sarimax_ampliado` |
| `parametro` | String | Nome técnico | Variável externa ou termo do modelo (`intercept`, `ar.L1`, `ar.S.L12`, `sigma2`) |
| `coeficiente` | Double | Conforme o parâmetro | Valor estimado |
| `erro_padrao` | Double | Conforme o parâmetro | Erro padrão da estimativa |
| `p_valor` | Double | Probabilidade | Significância estatística |
| `ic95_inferior` | Double | Conforme o parâmetro | Limite inferior do intervalo de 95% |
| `ic95_superior` | Double | Conforme o parâmetro | Limite superior do intervalo de 95% |
| `n_meses` | Long | Meses | Meses usados no ajuste |
| `run_id` | String | Identificador | Execução correspondente no MLflow |
| `treinado_em` | Timestamp | UTC | Momento do treino |

---

## 6.6 `projeto_hands_on.gold.metricas_modelo`

### Identificação

- **Finalidade:** comparar o erro dos modelos de previsão do IPCA na validação ano a ano.
- **Granularidade:** uma linha por alvo, modelo e período.
- **Chave:** `alvo` + `modelo` + `periodo`.
- **Origem:** notebook `treino_modelo_ipca.py`.
- **Tipo de objeto:** tabela Delta recriada a cada treino (não faz parte do pipeline).

### Colunas

| Coluna | Tipo | Unidade | Descrição |
|---|---|---|---|
| `alvo` | String | Código | `h1` a `h12` ou `acum_12m` |
| `horizonte` | Long | Meses | Meses entre o mês base e o mês previsto |
| `modelo` | String | Código | `ultimo_mes`, `media_movel_3m`, `media_movel_6m`, `media_movel_12m`, `sazonal`, `arima`, `sarimax`, `sarimax_ampliado`, `ridge` ou `random_forest` |
| `periodo` | String | Período do mês previsto | `2005 em diante`, `2005-2016`, `2017-2019`, `2020-2021` ou `2022 em diante` |
| `mae` | Double | p.p. | Erro absoluto médio |
| `rmse` | Double | p.p. | Raiz do erro quadrático médio |
| `n_teste` | Long | Previsões | Quantidade de previsões avaliadas |
| `run_id` | String | Identificador | Execução correspondente no MLflow |
| `treinado_em` | Timestamp | UTC | Momento do treino |

---

## 6.7 `projeto_hands_on.gold.previsoes_ipca`

### Identificação

- **Finalidade:** disponibilizar a previsão atual do IPCA e o previsto × realizado de cada modelo nos anos de teste.
- **Granularidade:** uma linha por tipo, mês base e alvo.
- **Chave:** `tipo` + `mes_base` + `alvo`.
- **Origem:** notebook `treino_modelo_ipca.py`.
- **Tipo de objeto:** tabela Delta recriada a cada treino (não faz parte do pipeline).
- **Consumidores:** simulador, sala do Genie e dashboard executivo (planejados).

### Colunas

| Coluna | Tipo | Unidade | Descrição |
|---|---|---|---|
| `tipo` | String | Código | `previsao` (a partir do mês base mais recente) ou `teste` (validação ano a ano) |
| `mes_base` | Date | Mês | Último mês com IPCA conhecido na previsão |
| `alvo` | String | Código | `h1` a `h12` ou `acum_12m` |
| `horizonte` | Long | Meses | Meses entre o mês base e o mês previsto |
| `mes_previsto` | Date | Mês | Mês previsto (no `acum_12m`, o último mês da janela de 12 meses) |
| `periodo` | String | Período | Período de leitura do resultado; vazio no tipo `previsao` |
| `valor_real` | Double | % | IPCA realizado; vazio no tipo `previsao` |
| `previsao_oficial` | Double | % | Previsão oficial, do modelo ARIMA |
| `limite_inferior`, `limite_superior` | Double | % | Faixa de 80% da previsão oficial, a partir dos erros do ARIMA nos testes; preenchida só no tipo `previsao` |
| `previsao_ridge` | Double | % | Previsão da regressão Ridge |
| `previsao_ultimo_mes`, `previsao_media_movel_3m`, `previsao_media_movel_6m`, `previsao_media_movel_12m`, `previsao_sazonal` | Double | % | Previsões das referências simples |
| `previsao_arima`, `previsao_sarimax`, `previsao_sarimax_ampliado` | Double | % | Previsões dos modelos de série temporal |
| `previsao_random_forest` | Double | % | Previsão do Random Forest |
| `run_id` | String | Identificador | Execução correspondente no MLflow |
| `treinado_em` | Timestamp | UTC | Momento do treino |

---

# 7. Matriz de consumo dos produtos

| Produto | Fonte principal | Finalidade |
|---|---|---|
| Notebook EDA no Databricks | Silver | Exploração, validação e análise técnica |
| Dashboard EDA no Power BI | Silver | Exploração visual e definição de indicadores |
| Monitoramento de qualidade | `silver.dq_resumo` | Unicidade, cobertura e atualização |
| Modelo de previsão do IPCA | `gold.features_mensal` | Machine learning e previsão h1 a h12 |
| Resultados do modelo e MLflow | `gold.previsoes_ipca`, `gold.metricas_modelo`, `gold.parametros_arima`, `gold.coeficientes_modelo` | Previsão atual, comparação de modelos e rastreabilidade dos treinos |
| Dashboard de negócio | `gold.bi_economia_mensal` planejada | Consumo executivo e indicadores mensais |

---

# 8. Regras de negócio consolidadas

## Selic

- Série 432.
- Unidade: percentual ao ano.
- Regra mensal: último valor disponível do mês.

## CDI

- Série 12.
- Unidade original: taxa diária.
- `valor`: média mensal da taxa diária anualizada com 252 dias úteis.
- `acumulado_mes`: composição das taxas diárias no mês.
- `dias_uteis`: quantidade de observações utilizadas.

## IPCA

- Série 433.
- Unidade: percentual no mês.
- Regra Silver: uma observação por `data_ref`, com deduplicação.

## TR

- Série 226.
- Unidade: taxa do período.
- Valores iguais a zero podem ser válidos.
- Para consumo mensal no BI, utiliza-se a média dos períodos iniciados no mês.

## Câmbio

- Cotações diárias em BRL por unidade da moeda.
- Análises diárias consomem a Silver.
- Fechamento mensal corresponde à última observação disponível no mês.
- Comparações entre moedas de escalas diferentes devem utilizar Base 100, variação percentual ou pequenos múltiplos.

---

# 9. Severidade das regras de qualidade

| Severidade | Comportamento |
|---|---|
| Registrar | Mantém o registro e contabiliza a violação |
| Descartar | Remove o registro inválido e contabiliza a violação |
| Interromper | Faz a atualização da tabela ou pipeline falhar |

---

# 10. Glossário

| Termo | Definição |
|---|---|
| BACEN / BCB | Banco Central do Brasil |
| SGS | Sistema Gerenciador de Séries Temporais |
| Selic | Taxa básica de juros da economia brasileira |
| CDI | Taxa de referência do mercado interbancário |
| IPCA | Índice Nacional de Preços ao Consumidor Amplo |
| TR | Taxa Referencial |
| a.a. | Ao ano |
| a.m. | Ao mês |
| p.p. | Pontos percentuais |
| Bronze | Camada de dados próximos à origem |
| Silver | Camada de dados limpos, tipados e validados |
| Gold | Camada orientada ao consumo analítico |
| Granularidade | Nível de detalhe representado por uma linha |
| Chave | Campo ou conjunto de campos que identifica uma linha |
| Materialized view | Resultado persistido e atualizado pelo pipeline |
| Streaming table | Tabela alimentada incrementalmente por novos dados |
| Expectation | Regra de qualidade executada pelo pipeline |
| Fechamento mensal | Última observação disponível no mês |
| Base 100 | Índice que normaliza a primeira observação para 100 |
| Lag | Valor defasado em relação ao período de referência |
| Horizonte hN | N meses após o mês base |
| Z-score | Valor expresso em desvios padrão em relação à média |
| Mês base | Último mês com IPCA divulgado, a partir do qual a previsão é feita |
| Validação ano a ano | Teste em que cada ano é previsto por modelos treinados só com dados até o fim do ano anterior |
| MAE | Erro absoluto médio: em média, quanto a previsão errou, para cima ou para baixo |
| ARIMA | Modelo de série temporal que prevê o IPCA a partir do próprio histórico |
| SARIMAX | ARIMA com variáveis externas, como juro real e variação do dólar |

---

# 11. Limitações e considerações

- As séries possuem datas iniciais, frequências e unidades diferentes.
- Meses recentes podem apresentar indicadores ainda não publicados.
- A atualização técnica da tabela não garante ausência de anomalias econômicas.
- Correlação não demonstra causalidade.
- Comparações entre taxa anual e taxa mensal devem ser evitadas sem transformação adequada.
- A tabela `gold.features_mensal` tem finalidade preditiva e não deve ser usada como substituta direta de uma Gold orientada ao BI.
- O dashboard exploratório consome Silver para preservar flexibilidade analítica.
- O dashboard executivo deverá consumir tabelas Gold após validação das regras de negócio.
- As tabelas de resultados do modelo (seções 6.4 a 6.7) são recriadas a cada treino e não passam pelas regras de qualidade do pipeline.
- Os resultados não constituem recomendação financeira.

---

# 12. Manutenção do catálogo

Este documento deve ser revisado quando ocorrer qualquer uma das situações abaixo:

- criação ou remoção de tabelas;
- inclusão, remoção ou renomeação de colunas;
- alteração de granularidade;
- mudança em regras de cálculo;
- alteração nos limites de qualidade;
- inclusão de novas séries SGS;
- criação de novos produtos consumidores.

## Histórico de versões

| Versão | Data | Alteração |
|---|---|---|
| 1.0 | 02/11/2026 | Criação inicial com Bronze, Silver, Gold existente e Gold BI planejada |
| 1.1 | 02/11/2026 | Inclusão do catálogo na documentação versionada do projeto, referência prevista no README e validação automatizada no CI. |
| 1.2 | 03/11/2026 | `gold.features_mensal` a partir de 01/2001 com validação ano a ano; inclusão das tabelas de resultados do modelo (6.4 a 6.7), com o ARIMA como modelo oficial, de termos no glossário e de teste no CI que verifica se todas as tabelas estão no catálogo. |
