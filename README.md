# Pipeline de Câmbio e Indicadores Econômicos (BACEN)

Pipeline de dados em camadas (bronze / silver) no Databricks que ingere cotações de câmbio e indicadores econômicos da API do Banco Central do Brasil, cobrindo todo o histórico disponível desde a data mais antiga de cada série (a partir de 1980) até a data atual. A carga histórica é feita uma única vez (até 31/12/2025, mais o complemento de 01/01/2026 até o dia da carga) e, a partir daí, uma ingestão diária agendada busca apenas os registros novos, sem repetição. A API do BACEN permite no máximo 10 anos por consulta, então os dados são extraídos em janelas de 10 anos.

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
      ├─ Bronze: streaming tables via Auto Loader
      │    bronze_moedas (5 fluxos append, um por moeda)
      │    bronze_selic, bronze_cdi, bronze_tr, bronze_ipca
      │
      └─ Silver: materialized views
           silver.moedas_conversao — moedas tipadas com códigos ISO
           silver.selic — mensal (valor vigente no último dia do mês, % a.a.)
           silver.cdi — mensal (média anualizada % a.a. + acumulado no mês)
           silver.tr — diário com data_ref + data_fim
           silver.ipca — variação percentual mensal
           silver.dq_resumo — resumo de qualidade (unicidade e atualização)
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
    └── silver/
        ├── silver_moedas_conversao.py  # Cotações de moedas tipadas
        ├── silver_indicadores.py       # Indicadores mensais (Selic, CDI), diários (TR), mensais (IPCA)
        └── silver_qualidade.py         # Resumo de qualidade (dq_resumo)
```

## Configuração

1. **Carga histórica (uma única vez):** executar `databricks/notebooks/ingestao_bacen.py` (ou disparar o Job sob demanda) para criar o catálogo `projeto_hands_on`, o schema `bronze` e o volume `raw`, buscar todo o histórico disponível da API do BACEN em janelas de 10 anos e complementar com os dados de 01/01/2026 até o dia da execução.
2. O Spark Declarative Pipeline (origem `databricks/transformations/`, catálogo `projeto_hands_on`, schema `bronze`, compute serverless) e os Jobs são criados pelo deploy do bundle — ver [CI/CD](#cicd).
3. Executar uma atualização do pipeline para materializar as tabelas bronze e silver.
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
| bronze (todas) | `data_valida`, `valor_numerico` | Registrar |
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

As faixas foram calibradas com o histórico completo: o dólar chegou a 71.153 (moeda anterior ao real), o IPCA a 82,39% em um mês (03/1990) e a TR é zero em boa parte do período recente, então valores altos ou zerados não são tratados como erro.

### Resumo de qualidade (`silver.dq_resumo`)

Tabela com uma linha por tabela silver, recalculada a cada atualização: `linhas`, `chaves_distintas`, `ultima_data`, `dias_desde_ultima_data`, `limite_atraso_dias` e `verificado_em`. Cobre o que as regras por registro não conseguem verificar:

- **Unicidade da chave** (como os meses duplicados do IPCA): chave `data_ref` nas tabelas mensais, `moeda` + `data_cotacao` no câmbio e `data_ref` + `data_fim` na TR.
- **Atualização dos dados:** até 7 dias para as séries diárias (feriados prolongados chegam a 5 dias sem cotação), 38 dias para Selic e CDI (datadas no dia 1 do mês) e 75 dias para o IPCA (publicado por volta do dia 10 do mês seguinte).

Os testes do CI garantem que toda tabela silver tenha regras de qualidade e esteja coberta pelo `dq_resumo`.

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
- **Reexecutar a carga histórica** apaga todos os arquivos do volume (inclusive os diários) e recria o histórico até o dia da execução. Nesse caso, é obrigatório fazer um *full refresh* do pipeline para não duplicar dados na bronze.
