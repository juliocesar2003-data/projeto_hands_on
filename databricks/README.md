# Pipeline de Câmbio e Indicadores Econômicos (BACEN) — Databricks

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
      ├─ Bronze: streaming tables via Auto Loader (com origem e data de ingestão)
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

## Conteúdo

```
notebooks/
├── ingestao_bacen.py            # Carga histórica one-off (BACEN API → NDJSON no UC Volume)
├── ingestao_diaria_bacen.py     # Ingestão diária incremental (somente registros novos)
├── eda_bacen.py                 # Análise exploratória sobre as tabelas silver (ver README principal)
└── bacen_series_explorer.py     # Notebook exploratório (últimos 10 registros por série SGS)
transformations/
├── bronze/
│   ├── bronze_moedas.py         # Streaming table de câmbio (5 fluxos append)
│   └── bronze_indicadores.py    # Streaming tables Selic/CDI/TR/IPCA
└── silver/
    ├── silver_moedas_conversao.py  # Cotações de moedas tipadas
    ├── silver_indicadores.py       # Indicadores mensais (Selic, CDI), diários (TR), mensais (IPCA)
    └── silver_qualidade.py         # Resumo de qualidade (dq_resumo)
```

## Configuração

1. **Carga histórica (uma única vez):** executar `notebooks/ingestao_bacen.py` (ou disparar o Job sob demanda) para criar o catálogo `projeto_hands_on`, o schema `bronze` e o volume `raw`, buscar todo o histórico disponível da API do BACEN em janelas de 10 anos e complementar com os dados de 01/01/2026 até o dia da execução.
2. O Spark Declarative Pipeline (origem `transformations/`, catálogo `projeto_hands_on`, schema `bronze`, compute serverless) e os Jobs são criados pelo deploy do bundle (`databricks.yml` na raiz do repositório) — ver a seção *CI/CD* do [README principal](../README.md#cicd).
3. Executar uma atualização do pipeline para materializar as tabelas bronze e silver.
4. **Ingestão diária:** o Job agendado descrito na seção [Jobs](#jobs) também é criado pelo deploy do bundle.

## Jobs

Os Jobs e o pipeline são definidos como código em `databricks.yml` (raiz do repositório) e criados/atualizados automaticamente pelo CD.

### Carga histórica (sob demanda) — `carga_historica_bacen`

Executa `notebooks/ingestao_bacen.py` (serverless, disparo manual), apenas na primeira carga ou para reconstruir o histórico.

### Ingestão diária (agendada) — `ingestao_diaria_bacen` (*Ingestão Diária BACEN (Agendada)* no Databricks)

Job para rodar sem acompanhamento, com a seguinte configuração:

| Configuração | Valor |
|---|---|
| Tarefa 1 — `ingestao_diaria` | Notebook `notebooks/ingestao_diaria_bacen.py`, compute serverless |
| Tarefa 2 — `atualiza_pipeline` | Tipo *Pipeline*, apontando para o Spark Declarative Pipeline, dependente da tarefa 1 com condição *All done* (atualiza o que foi gravado mesmo se uma série falhar) |
| Agendamento | Diário às 21:00, fuso `America/Sao_Paulo` (após a publicação da PTAX e dos indicadores do dia) |
| Retentativas | 2 retentativas na tarefa 1, com intervalo de 10 minutos |
| Notificações | E-mail em caso de falha (para o usuário dono do token usado no deploy) |
| Execuções simultâneas | No máximo 1 |

Como a ingestão diária sempre parte da última data gravada no volume, dias com falha ou sem execução são recuperados automaticamente na execução seguinte.

## Qualidade de dados

As regras de qualidade (*expectations*) e a tabela `silver.dq_resumo` estão descritas na seção *Qualidade de dados* do [README principal](../README.md#qualidade-de-dados).

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

## Observações

- **Sem repetição:** a ingestão diária lê a última data de cada série diretamente dos arquivos do volume, busca a partir do dia seguinte e mantém apenas registros posteriores a essa data e até hoje. Cada execução grava um arquivo novo (`<serie>_diario_<timestamp>.json`) e nunca sobrescreve arquivos, então o Auto Loader ingere cada registro uma única vez.
- A API do BACEN devolve registros fora do intervalo pedido (séries mensais retornam o mês anterior a `dataInicial`; a Selic meta publica datas futuras até a próxima reunião do Copom) e às vezes responde "sem dados" com HTTP 200. Os notebooks tratam esses casos; datas futuras não são gravadas.
- **IPCA:** a carga histórica em janelas de 10 anos gravou em duplicidade o mês de início de cada janela (01/1990, 01/2000, 01/2010 e 01/2020), porque em séries mensais a API devolve também o mês anterior a `dataInicial`. A `silver.ipca` remove essas duplicidades (`dropDuplicates` por `data_ref`).
- **Metadados de ingestão em registros antigos:** as colunas de linhagem só são preenchidas nos registros lidos depois da sua criação. Para preenchê-las em todo o histórico, é preciso um *full refresh* das tabelas bronze, que relê todos os arquivos do volume (testado em um pipeline temporário: mesmas quantidades de linhas antes e depois).
- **Reexecutar a carga histórica** apaga todos os arquivos do volume (inclusive os diários) e recria o histórico até o dia da execução. Nesse caso, é obrigatório fazer um *full refresh* do pipeline para não duplicar dados na bronze.
