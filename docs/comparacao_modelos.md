# Comparação dos modelos de previsão do IPCA

## Como os modelos foram testados

- **Objetivo:** prever o IPCA de cada um dos próximos 12 meses e o acumulado em 12 meses.
- **Validação ano a ano (2005 a 2026):** cada ano foi previsto por modelos treinados só com dados até o fim do ano anterior, como funcionaria na prática. São cerca de 250 previsões por horizonte.
- **Métrica:** erro absoluto médio (MAE), em pontos percentuais. Quanto menor, melhor.

## Resultado

Erro médio de h1 a h12 (p.p.), por período do mês previsto. Em negrito, o melhor de cada coluna.

| Modelo | O que faz | 2005–2026 | 2005–2016 | 2017–2019 | 2020–2021 | 2022 em diante |
|---|---|---|---|---|---|---|
| **ARIMA** | Combina o mês anterior e o mesmo mês do ano anterior, com pesos estimados | **0,247** | **0,202** | 0,295 | **0,368** | **0,274** |
| Média sazonal | Média histórica do mês do calendário | 0,260 | 0,212 | 0,317 | 0,418 | **0,274** |
| Média móvel 12 meses | Média do último ano | 0,267 | 0,221 | 0,281 | 0,400 | 0,312 |
| Ridge | Regressão com IPCA, Selic, dólar e meta | 0,276 | 0,244 | 0,300 | 0,409 | 0,285 |
| Média móvel 6 meses | Média do último semestre | 0,279 | 0,239 | **0,259** | 0,385 | 0,340 |
| SARIMAX | ARIMA com juro real e dólar | 0,292 | 0,276 | 0,261 | 0,415 | 0,301 |
| Média móvel 3 meses | Média do último trimestre | 0,297 | 0,257 | 0,262 | 0,403 | 0,373 |
| Mesmo mês do ano anterior | Repete o IPCA do mesmo mês, um ano antes | 0,299 | 0,226 | 0,366 | 0,473 | 0,362 |
| Random Forest | Árvores de decisão com 24 variáveis | 0,315 | 0,266 | 0,315 | 0,369 | 0,410 |
| Último mês | Repete o IPCA mais recente | 0,322 | 0,273 | 0,297 | 0,433 | 0,411 |

No IPCA acumulado em 12 meses, a ordem se mantém: ARIMA 1,74 p.p., média sazonal 1,95, último mês 2,01, Ridge 2,41, SARIMAX 2,54 e Random Forest 3,22.

## Modelo escolhido: ARIMA

O ARIMA escolhido é o SARIMA(1,0,0)(1,0,0,12): o IPCA de um mês depende do mês anterior (peso 0,60) e do mesmo mês do ano anterior (peso 0,20). Ele foi escolhido porque:

- **tem o menor erro no período todo** e em três dos quatro períodos, em todos os horizontes;
- **junta as duas regras simples:** a inércia do último mês e a sazonalidade do mesmo mês do ano anterior, com pesos estimados nos dados. Por isso erra menos que cada uma delas sozinha;
- **é simples e estável:** tem três parâmetros, todos significativos, e usa só o histórico do IPCA.

## Métricas de teste do ARIMA

| Horizonte | Previsões | MAE (p.p.) | RMSE (p.p.) | Viés (p.p.) | R² | Redução do MAE sobre "último mês" |
|---|---|---|---|---|---|---|
| Mês seguinte (h1) | 260 | 0,206 | 0,280 | −0,021 | 0,29 | 12,3% |
| 2 meses à frente (h2) | 259 | 0,235 | 0,313 | −0,037 | 0,12 | 19,1% |
| 3 meses à frente (h3) | 258 | 0,244 | 0,327 | −0,047 | 0,03 | 25,2% |

- **MAE:** erro médio, para cima ou para baixo.
- **RMSE:** como o MAE, mas pune mais os erros grandes.
- **Viés:** real menos previsto. Negativo indica previsão, em média, acima do realizado.
- **R²:** quanto o modelo explica além da média do período de teste. A capacidade de explicação cai rápido: 0,29 no mês seguinte e quase zero em 3 meses.

## Por que não os outros

- **Médias móveis:** as curtas (3 e 6 meses) foram as melhores em 2017–2019, quando a inflação caiu, porque se adaptam rápido. Nos outros períodos erram mais.
- **Mesmo mês do ano anterior:** funciona em anos estáveis, mas falha quando a inflação muda de patamar. Em 2017–2019 repetia os meses de inflação alta de 2015–2016.
- **Ridge:** precisa de muitos anos de treino e ficou em 4º lugar. O coeficiente dos juros saiu com o sinal contrário ao esperado.
- **SARIMAX (com Selic e dólar):** piorou a previsão no geral. O efeito do dólar tem o sinal esperado; o dos juros não é mensurável com esses dados, porque o Banco Central sobe a Selic quando a inflação sobe.
- **Random Forest:** com cerca de 300 meses de dados, decora o passado e não projeta valores fora do que já viu.

## Previsão atual (mês base: setembro/2026)

| Mês previsto | ARIMA | Faixa de 80% |
|---|---|---|
| Outubro/2026 | 0,61% | 0,25% a 0,92% |
| Acumulado de outubro/2026 a setembro/2027 | 6,27% | 3,09% a 8,61% |

## Onde conferir

- Notebook `databricks/notebooks/treino_modelo_ipca.py` e experimento `previsao_ipca` no MLflow.
- Tabelas `gold.metricas_modelo` (erros) e `gold.previsoes_ipca` (previsão oficial e previsto × realizado).
- O "mesmo mês do ano anterior" foi calculado sobre as mesmas previsões de teste, a partir de `silver.ipca`.

Números da validação de 03/11/2026. Os valores mudam a cada novo IPCA divulgado.
