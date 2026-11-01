# Pipeline de Câmbio e Indicadores Econômicos (BACEN)

Pipeline de dados em camadas (bronze / silver / gold) no Databricks que ingere cotações de câmbio e indicadores econômicos da API do Banco Central do Brasil, cobrindo todo o histórico disponível desde a data mais antiga de cada série (a partir de 1980) até a data atual. A carga histórica é feita uma única vez (até 31/12/2025, mais o complemento de 01/01/2026 até o dia da carga) e, a partir daí, uma ingestão diária agendada busca apenas os registros novos, sem repetição. A API do BACEN permite no máximo 10 anos por consulta, então os dados são extraídos em janelas de 10 anos.

## Arquitetura

```
BACEN API (9 séries SGS)
      │
      ▼
Carga histórica (one-off) ──── Job sob demanda (serverless)
   ingestao_bacen.py: histórico até 31/12/2025 + 01/01/2026 até hoje
      │
Ingestão diária ──────────────── Job agendado diário (serverless)
   ingestao_diaria_bacen.py: da última data gravada até hoje
      │
      ▼
UC Volume: projeto_hands_on.bronze.raw (arquivos NDJSON por série)
      │
      ▼
Spark Declarative Pipeline (serverless)
      │
      ├─ Bronze: streaming tables via Auto Loader (com origem e data de ingestão)
      │    bronze_moedas (5 fluxos append, um por moeda)
      │    bronze_selic, bronze_cdi, bronze_tr, bronze_ipca
      │
      ├─ Silver: materialized views
      │    silver.moedas_conversao — moedas tipadas com códigos ISO
      │    silver.selic — mensal (valor vigente no último dia do mês, % a.a.)
      │    silver.cdi — mensal (média anualizada % a.a. + acumulado no mês)
      │    silver.tr — diário com data_ref + data_fim
      │    silver.ipca — variação percentual mensal
      │    silver.dq_resumo — resumo de qualidade (unicidade e atualização)
      │
      └─ Gold: materialized views
           gold.metas_inflacao — metas de inflação do CMN por ano
           gold.features_mensal — base do modelo de previsão do IPCA (features + alvos h1 a h12)
```

## Estrutura do repositório

```
.github/workflows/
├── ci.yml                           # CI: lint, testes e validação do bundle em push/PR
└── cd.yml                           # CD: deploy no Databricks a cada merge na main
databricks.yml                       # Databricks Asset Bundle (pipeline + jobs como código)
pyproject.toml                       # Configuração do ruff e do pytest
requirements-dev.txt                 # Dependências de desenvolvimento (CI e testes locais)
tests/                               # Testes unitários (rodam sem Databricks)
databricks/
├── notebooks/
│   ├── ingestao_bacen.py            # Carga histórica one-off (BACEN API → NDJSON no UC Volume)
│   ├── ingestao_diaria_bacen.py     # Ingestão diária incremental (somente registros novos)
│   ├── eda_bacen.py                 # Análise exploratória sobre as tabelas silver
│   └── bacen_series_explorer.py     # Notebook exploratório (últimos 10 registros por série SGS)
└── transformations/
    ├── bronze/
    │   ├── bronze_moedas.py         # Streaming table de câmbio (5 fluxos append)
    │   └── bronze_indicadores.py    # Streaming tables Selic/CDI/TR/IPCA
    ├── silver/
    │   ├── silver_moedas_conversao.py  # Cotações de moedas tipadas
    │   ├── silver_indicadores.py       # Indicadores mensais (Selic, CDI), diários (TR), mensais (IPCA)
    │   └── silver_qualidade.py         # Resumo de qualidade (dq_resumo)
    └── gold/
        └── gold_features.py            # Metas de inflação e base do modelo (features_mensal)
```

## Configuração

1. **Carga histórica (uma única vez):** executar `databricks/notebooks/ingestao_bacen.py` (ou disparar o Job sob demanda) para criar o catálogo `projeto_hands_on`, o schema `bronze` e o volume `raw`, buscar todo o histórico disponível da API do BACEN em janelas de 10 anos e complementar com os dados de 01/01/2026 até o dia da execução.
2. O Spark Declarative Pipeline (origem `databricks/transformations/`, catálogo `projeto_hands_on`, schema `bronze`, compute serverless) e os Jobs são criados pelo deploy do bundle — ver [CI/CD](#cicd).
3. Executar uma atualização do pipeline para materializar as tabelas bronze, silver e gold.
4. **Ingestão diária:** o Job agendado descrito na seção [Jobs](#jobs) também é criado pelo deploy do bundle.

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

### Regras de agregação mensal (silver)

| Série | Regra | Unidade |
|---|---|---|
| Selic | Valor vigente no último dia do mês | % a.a. |
| CDI — `valor` | Taxa diária anualizada por `(1 + taxa/100)^252 − 1`, com média no mês | % a.a. (comparável à Selic) |
| CDI — `acumulado_mes` | Taxas diárias compostas no mês: `∏(1 + taxa/100) − 1` | % no mês (comparável ao IPCA) |
| CDI — `dias_uteis` | Quantidade de dias com taxa no mês | dias |

O MAX mensal usado anteriormente distorcia os dados: nos meses com mudança da Selic pelo Copom, pegava a taxa anterior ao corte (ou a nova taxa, no caso de alta), e o CDI ficava em % ao dia, sem comparação possível com Selic e IPCA. Exemplo, agosto/2023 (corte de 13,75% para 13,25% em 02/08): a Selic passa de 13,75 para 13,25, e o CDI de 0,050788 (% a.d.) para 13,19% a.a. e 1,1375% no mês. O mês corrente é parcial: `acumulado_mes` reflete os dias úteis já publicados.

## Qualidade de dados

As regras de qualidade são *expectations* do Spark Declarative Pipeline: rodam sobre os dados reais a cada atualização, e o resultado de cada regra (registros aprovados e reprovados) fica no log de eventos e na aba *Data quality* de cada tabela do pipeline.

### Níveis de severidade

| Nível | Decorador | O que acontece na violação | Quando é usado |
|---|---|---|---|
| Registrar | `@dp.expect` / `expect_all` | O registro é mantido e a violação é contabilizada | Faixas de valores e dados brutos (bronze) |
| Descartar | `@dp.expect_or_drop` | O registro é removido da tabela e contabilizado | Registros inutilizáveis (cotação zerada ou negativa) |
| Interromper | `@dp.expect_or_fail` | A atualização do pipeline falha e o Job envia o e-mail de falha | Erros de carga ou de mapeamento que tornariam os dados errados |

### Regras por tabela

| Tabela | Regra | Nível |
|---|---|---|
| bronze (todas) | `data_valida`, `valor_numerico`, `origem_ingestao_preenchida` | Registrar |
| `bronze_moedas` | `cotacao_positiva` | Registrar |
| `bronze_tr` | `periodo_valido` (`dataFim` > `data`) | Registrar |
| `silver.moedas_conversao` | `moeda_conhecida`, `data_cotacao_valida` | Interromper |
| `silver.moedas_conversao` | `cotacao_positiva` | Descartar |
| `silver.selic` / `cdi` / `tr` / `ipca` | `data_ref_valida` | Interromper |
| `silver.selic` | `faixa_plausivel` (0 a 100% a.a.) | Registrar |
| `silver.cdi` | `taxas_nao_negativas`, `dias_uteis_no_mes` (1 a 23) | Registrar |
| `silver.tr` | `taxa_nao_negativa`, `periodo_valido` | Registrar |
| `silver.ipca` | `faixa_plausivel` (−5% a 100% no mês) | Registrar |
| `silver.dq_resumo` | `chave_unica` (sem chaves duplicadas) | Interromper |
| `silver.dq_resumo` | `dados_atualizados` (atraso dentro do limite) | Registrar |
| `gold.metas_inflacao` | `ano_valido`, `meta_positiva` | Interromper |
| `gold.features_mensal` | `mes_base_valido` | Interromper |
| `gold.features_mensal` | `features_completas`, `meta_cadastrada`, `ipca_plausivel` (−3% a 5% no mês), `selic_plausivel` (0 a 50% a.a.), `dolar_plausivel` | Registrar |

As faixas foram calibradas com o histórico completo: o dólar chegou a 71.153 (moeda anterior ao real), o IPCA a 82,39% em um mês (03/1990) e a TR é zero em boa parte do período recente, então valores altos ou zerados não são tratados como erro.

### Metadados de ingestão (bronze)

Todas as tabelas bronze têm três colunas de linhagem, preenchidas pelo Auto Loader a partir dos metadados do arquivo lido:

| Coluna | Origem | Significado |
|---|---|---|
| `origem_ingestao` | `_metadata.file_path` | Caminho do arquivo no volume. O nome indica a carga: `<serie>_<ano>_2025.json` (histórica), `<serie>_2026_<data>.json` (complemento de 2026) ou `<serie>_diario_<timestamp>.json` (diária) |
| `data_ingestao` | `_metadata.file_modification_time` | Quando o arquivo foi gravado no volume pela ingestão (UTC). Não muda se a bronze for reprocessada |
| `data_processamento` | `current_timestamp()` | Quando o pipeline leu o registro para a bronze (UTC). Muda em um *full refresh* |

A regra de qualidade `origem_ingestao_preenchida` (nível registrar) verifica que toda linha nova tenha a origem preenchida.

### Resumo de qualidade (`silver.dq_resumo`)

Tabela com uma linha por tabela silver, recalculada a cada atualização: `linhas`, `chaves_distintas`, `ultima_data`, `dias_desde_ultima_data`, `limite_atraso_dias` e `verificado_em`. Cobre o que as regras por registro não conseguem verificar:

- **Unicidade da chave** (como os meses duplicados do IPCA): chave `data_ref` nas tabelas mensais, `moeda` + `data_cotacao` no câmbio e `data_ref` + `data_fim` na TR.
- **Atualização dos dados:** até 7 dias para as séries diárias (feriados prolongados chegam a 5 dias sem cotação), 38 dias para Selic e CDI (datadas no dia 1 do mês) e 75 dias para o IPCA (publicado por volta do dia 10 do mês seguinte).

Os testes do CI garantem que toda tabela silver e gold tenha regras de qualidade e esteja coberta pelo `dq_resumo`.

## Análise exploratória

O notebook `databricks/notebooks/eda_bacen.py` faz a análise exploratória lendo diretamente as tabelas `projeto_hands_on.silver`, então os resultados acompanham a ingestão diária. O período é definido pelos parâmetros `data_inicial` (padrão `2020-01-01`) e `data_final` (vazio = último mês completo).

Conteúdo:

- qualidade e cobertura das tabelas (nulos e chaves duplicadas);
- base econômica mensal (Selic e CDI da silver, TR média mensal, IPCA e câmbio de fim de mês);
- USD/BRL diário com média móvel, extremos e comparação das cinco moedas (Base 100, variação acumulada e volatilidade);
- Selic × CDI, Selic × IPCA, juro real aproximado (CDI acumulado no mês − IPCA) e defasagem entre Selic e IPCA;
- sazonalidade do IPCA por mês do calendário;
- matriz de correlação em nível e em variação mensal;
- conclusões e implicações para a camada gold e o modelo.

Para executar, abra o notebook no workspace (ele é publicado pelo deploy do bundle em `.bundle/projeto_hands_on/prod/files/databricks/notebooks/`) e rode todas as células com compute serverless.

## Camada gold — base do modelo (`gold.features_mensal`)

Tabela larga (*wide table*) usada para treinar o modelo de previsão do IPCA e alimentar o simulador. Fica em `databricks/transformations/gold/gold_features.py` e é recalculada a cada atualização do pipeline.

### Granularidade: mês base

Cada linha é um **mês base**: um mês com IPCA já divulgado. As features usam apenas dados até o fim do mês base, e os alvos são o IPCA dos 12 meses seguintes (`h1` é o primeiro mês ainda sem IPCA divulgado). Assim, a previsão sempre usa os dados disponíveis no dia da consulta:

| Dia da consulta | Último IPCA divulgado | Mês base da previsão | `h1` prevê |
|---|---|---|---|
| 5 de novembro | setembro (outubro sai por volta do dia 10) | setembro | outubro |
| 11 de novembro | outubro (entra na carga diária das 21h do dia da divulgação) | outubro | novembro |

No treino não há vazamento: cada linha só usa dados que já existiam quando o IPCA do mês base foi divulgado. Os dias de câmbio e Selic entre o fim do mês base e o dia da consulta ficam de fora e entram pelo simulador.

### Período e conjuntos

Meses base de 01/2006 até o último IPCA divulgado (20 anos). A coluna `conjunto` separa:

| Conjunto | Meses base | Uso |
|---|---|---|
| `treino` | 01/2006 a 12/2019 | Ajuste do modelo e cálculo dos z-scores |
| `teste` | 01/2020 em diante, com pelo menos um alvo conhecido | Avaliação do erro por horizonte |
| `previsao` | Último mês base (nenhum alvo conhecido) | Previsão atual |

### Colunas

Nomenclatura: `lagN` = N meses antes do mês previsto em `h1` (`lag1` é o próprio mês base); `hN` = N meses à frente do mês base; janelas como `12m` indicam o período acumulado.

| Grupo | Colunas | Unidade |
|---|---|---|
| Controle | `mes_base`, `mes_h1`, `mes_calendario`, `alvos_disponiveis`, `features_completas`, `conjunto`, `data_calculo` | — |
| IPCA | `ipca_lag1`, `ipca_lag2`, `ipca_lag3`, `ipca_lag12`, `ipca_acum_3m`, `ipca_acum_12m` | % |
| Sazonalidade | `ipca_sazonal_h1` … `ipca_sazonal_h12` (média histórica do mês do calendário de cada horizonte, desde 2000, só com anos já conhecidos) e `ipca_menos_sazonal_lag1` (IPCA do mês base − média dos anos anteriores para o mesmo mês) | % / p.p. |
| Meta de inflação | `meta_inflacao`, `ipca_12m_menos_meta` | % / p.p. |
| Selic | `selic_lag1` (fim do mês base), `selic_var_3m_pp`, `selic_var_12m_pp`, `meses_desde_mudanca_selic` | % a.a. / p.p. / meses |
| Juro real | `juro_real_12m` = Selic − IPCA acumulado em 12 meses | p.p. |
| Dólar | `usd_lag1` (fechamento do mês), `usd_var_lag1`, `usd_var_3m`, `usd_var_12m`, `usd_volatilidade_lag1` (desvio padrão dos retornos diários no mês) | R$ / % |
| Dólar × IPCA | `usd_menos_ipca_12m` = variação do dólar em 12 meses − IPCA em 12 meses | p.p. |
| Padronizadas | `usd_var_12m_z`, `selic_var_12m_pp_z`, `ipca_acum_12m_z` | desvios padrão |
| Comparações | `usd_vs_selic_z`, `ipca_vs_selic_z`, `usd_vs_ipca_z` | desvios padrão |
| Alvos | `alvo_ipca_h1` … `alvo_ipca_h12`, `alvo_ipca_acum_12m` | % |

**Comparações entre indicadores:** o dólar varia em % e a Selic em pontos percentuais, então a diferença direta não tem significado. Cada variação é padronizada em z-score (`(valor − média) / desvio padrão`, com média e desvio calculados só no período de treino), e a comparação é a diferença dos z-scores. Exemplo: `usd_vs_selic_z = 1,5` significa que, em 12 meses, o dólar subiu 1,5 desvio padrão a mais (em relação ao normal dele) do que a Selic (em relação ao normal dela).

### Metas de inflação (`gold.metas_inflacao`)

Tabela de referência com a meta do CMN por ano (`ano`, `meta`, `tolerancia_pp`, `limite_inferior`, `limite_superior`, `norma`), mantida no código (`METAS_INFLACAO`) a partir do [histórico do Banco Central](https://www.bcb.gov.br/controleinflacao/historicometas). De 2006 a 2018 a meta foi 4,50%, caindo até 3,00% em 2024. Desde 2025 vale a meta contínua de 3,00% ± 1,5 p.p. (Resolução CMN nº 5.141), medida no IPCA acumulado em 12 meses. **Quando o CMN definir a meta de um novo ano, é preciso incluí-la em `METAS_INFLACAO`**; a regra de qualidade `meta_cadastrada` sinaliza meses sem meta.

## Jobs

Os Jobs e o pipeline são definidos como código em `databricks.yml` e criados/atualizados automaticamente pelo CD. Não é necessário configurá-los pela interface do Databricks.

### Carga histórica (sob demanda) — `carga_historica_bacen`

Executa `databricks/notebooks/ingestao_bacen.py` (serverless, disparo manual), apenas na primeira carga ou para reconstruir o histórico.

### Ingestão diária (agendada) — `ingestao_diaria_bacen` (*Ingestão Diária BACEN (Agendada)* no Databricks)

Job para rodar sem acompanhamento, com a seguinte configuração:

| Configuração | Valor |
|---|---|
| Tarefa 1 — `ingestao_diaria` | Notebook `databricks/notebooks/ingestao_diaria_bacen.py`, compute serverless |
| Tarefa 2 — `atualiza_pipeline` | Tipo *Pipeline*, apontando para o Spark Declarative Pipeline, dependente da tarefa 1 com condição *All done* (atualiza o que foi gravado mesmo se uma série falhar) |
| Agendamento | Diário às 21:00, fuso `America/Sao_Paulo` (após a publicação da PTAX e dos indicadores do dia) |
| Retentativas | 2 retentativas na tarefa 1, com intervalo de 10 minutos |
| Notificações | E-mail em caso de falha (para o usuário dono do token usado no deploy) |
| Execuções simultâneas | No máximo 1 |

Como a ingestão diária sempre parte da última data gravada no volume, dias com falha ou sem execução são recuperados automaticamente na execução seguinte.

## CI/CD

```
push / pull request ──→ CI (GitHub Actions)
                          ├─ ruff: lint do código Python
                          ├─ pytest: testes unitários (sem Databricks)
                          └─ databricks bundle validate
                                │ merge na main
                                ▼
                        CD (GitHub Actions)
                          ├─ roda o CI novamente
                          └─ databricks bundle deploy -t prod
                               → notebooks, pipeline e Jobs atualizados no workspace
```

### CI — Integração Contínua (`.github/workflows/ci.yml`)

Roda em todo push para branches diferentes da `main` e em todo pull request para a `main`:

- **Lint (`ruff`):** detecta erros reais no código (nomes indefinidos, imports não usados, erros de sintaxe). `spark`, `dbutils` e `display` são declarados como objetos do runtime do Databricks.
- **Testes (`pytest`):** as funções puras são extraídas do código-fonte dos notebooks, sem executá-los e sem depender do Databricks, e testadas com respostas simuladas da API:
  - `tests/test_ingestao_diaria.py`: janelas de 10 anos, filtro que impede repetição (registros já gravados, datas futuras, duplicatas) e tratamento das respostas da API (HTTP 404, "sem dados" com HTTP 200, erros de conexão e retentativas).
  - `tests/test_consistencia_projeto.py`: cada série tem exatamente um `pathGlobFilter` no pipeline, os nomes dos arquivos gravados caem no glob correto, a carga histórica e a diária usam as mesmas séries, os notebooks mantêm o formato Databricks e o `databricks.yml` aponta para arquivos existentes.
- **Validação do bundle:** `databricks bundle validate` confere o `databricks.yml` no workspace. É ignorada (com aviso) enquanto os secrets não estiverem configurados.

### CD — Entrega Contínua (`.github/workflows/cd.yml`)

Roda a cada merge na `main` (ou manualmente em *Actions > CD > Run workflow*). Executa o CI e, se passar, faz `databricks bundle deploy -t prod`, que publica os notebooks e cria/atualiza o pipeline `pipeline_bacen` (*Pipeline Câmbio - BACEN* no Databricks) e os Jobs `carga_historica_bacen` e `ingestao_diaria_bacen` (*Ingestão Diária BACEN (Agendada)*). O deploy não executa a carga nem o pipeline; ele só atualiza as definições.

### Configuração inicial (uma única vez)

1. **Token do Databricks:** em *Settings > Developer > Access tokens* do workspace, gerar um token de acesso. Para produção, o recomendado é usar um *service principal* em vez de um usuário pessoal.
2. **Secrets no GitHub:** em *Settings > Secrets and variables > Actions* do repositório, criar:
   - `DATABRICKS_HOST`: URL do workspace (ex.: `https://<workspace>.cloud.databricks.com`).
   - `DATABRICKS_TOKEN`: o token gerado no passo 1.
3. **Vincular o pipeline existente ao bundle (obrigatório se o pipeline já foi criado pela interface):** as tabelas bronze/silver pertencem ao pipeline atual, e um pipeline novo criado pelo bundle falharia ao tentar gerenciar as mesmas tabelas. Com a [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) autenticada com o **mesmo usuário/token** do passo 1, rodar na raiz do repositório:

   ```bash
   databricks bundle deployment bind pipeline_bacen <ID_DO_PIPELINE_ATUAL> -t prod
   ```

   O ID aparece na página do pipeline no Databricks. **Não exclua o pipeline atual**: excluir um pipeline apaga as tabelas que ele gerencia.
4. **Jobs criados pela interface:** se já existir um Job da carga histórica ou da ingestão diária criado manualmente, vincule-o da mesma forma (`databricks bundle deployment bind carga_historica_bacen <ID_DO_JOB> -t prod` / `databricks bundle deployment bind ingestao_diaria_bacen <ID_DO_JOB> -t prod`) ou exclua-o depois do primeiro deploy. **Não deixe dois Jobs de ingestão diária ativos**: rodando ao mesmo tempo, os dois leem a mesma última data e gravam os mesmos registros.
5. **Proteger a `main`:** em *Settings > Branches*, criar uma regra para a `main` exigindo pull request e o check *Lint e testes* aprovado antes do merge.

### Rodar o CI localmente

```bash
pip install -r requirements-dev.txt
ruff check .
pytest -v
```

## Observações

- O período de ingestão cobre desde a data mais antiga disponível de cada série (IPCA desde 01/01/1980, dolar desde 28/11/1984, CDI desde 06/03/1986, etc.) até a data atual.
- Os arquivos NDJSON são gravados no Unity Catalog Volume `projeto_hands_on.bronze.raw`.
- O pipeline utiliza Auto Loader para detectar novos arquivos automaticamente.
- O catálogo, schema e volume são criados automaticamente na primeira execução do notebook.
- **Sem repetição:** a ingestão diária lê a última data de cada série diretamente dos arquivos do volume, busca a partir do dia seguinte e mantém apenas registros posteriores a essa data e até hoje. Cada execução grava um arquivo novo (`<serie>_diario_<timestamp>.json`) e nunca sobrescreve arquivos, então o Auto Loader ingere cada registro uma única vez.
- A API do BACEN devolve registros fora do intervalo pedido (séries mensais retornam o mês anterior a `dataInicial`; a Selic meta publica datas futuras até a próxima reunião do Copom) e às vezes responde "sem dados" com HTTP 200. Os notebooks tratam esses casos; datas futuras não são gravadas.
- **IPCA:** a carga histórica em janelas de 10 anos gravou em duplicidade o mês de início de cada janela (01/1990, 01/2000, 01/2010 e 01/2020), porque em séries mensais a API devolve também o mês anterior a `dataInicial`. A `silver.ipca` remove essas duplicidades (`dropDuplicates` por `data_ref`).
- **Metadados de ingestão em registros antigos:** as colunas de linhagem só são preenchidas nos registros lidos depois da sua criação. Para preenchê-las em todo o histórico, é preciso um *full refresh* das tabelas bronze, que relê todos os arquivos do volume (testado em um pipeline temporário: mesmas quantidades de linhas antes e depois).
- **Reexecutar a carga histórica** apaga todos os arquivos do volume (inclusive os diários) e recria o histórico até o dia da execução. Nesse caso, é obrigatório fazer um *full refresh* do pipeline para não duplicar dados na bronze.
