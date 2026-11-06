# Resultados do modelo de previsão do IPCA

## Como o modelo foi testado

- **Objetivo:** prever o IPCA de cada um dos próximos 12 meses e o acumulado em 12 meses.
- **Validação ano a ano (2005 a 2026):** cada ano foi previsto por modelos treinados só com dados até o fim do ano anterior. São cerca de 250 previsões por horizonte.
- **Sem olhar para o futuro:** a previsão e o Focus usam só as pesquisas publicadas até o dia 5 do mês seguinte ao mês base.
- **Métricas:** erro absoluto médio (MAE), em pontos percentuais, e R², que mede quanto o modelo explica além da média do período.

## Resultado

| | Erro médio h1–h3 (p.p.) | R² h1 | R² h2 | R² h3 | R² h12 |
|---|---|---|---|---|---|
| **Previsão** | **0,193** | **0,63** | **0,27** | **0,18** | **0,09** |
| Focus | 0,199 | 0,55 | 0,22 | 0,15 | 0,04 |

No IPCA acumulado em 12 meses, o MAE é 1,45 p.p. na previsão e 1,52 no Focus.

A previsão tem números melhores no período todo, mas a diferença para o Focus não é estatisticamente significativa em nenhum horizonte testado (1, 2, 3, 6 e 12 meses e acumulado).

## Por período

| Período | Erro médio h1–h3: previsão | Erro médio h1–h3: Focus | R² h1: previsão | R² h1: Focus |
|---|---|---|---|---|
| 2005–2016 | **0,155** | 0,163 | **0,66** | 0,57 |
| 2017–2019 | 0,208 | **0,202** | **0,45** | 0,33 |
| 2020–2021 (pandemia) | **0,329** | 0,380 | **0,72** | 0,41 |
| 2022 em diante | 0,218 | **0,211** | 0,56 | **0,62** |

## Modelo oficial

```
Previsão base = (ARIMA + Focus + Top 5) ÷ 3
h1:        previsão base + intercepto + coeficiente × revisão do Top 5 em 4 semanas
h2 a h12:  previsão base
```

- **ARIMA:** SARIMA(1,0,0)(1,0,0,12), em que o IPCA de um mês depende do mês anterior e do mesmo mês do ano anterior.
- **Focus e Top 5:** expectativas de mercado publicadas pelo Banco Central. Trazem informação que não está no histórico do IPCA, como reajustes anunciados.
- **Correção:** o mercado costuma reagir devagar às notícias, então a revisão recente ajuda a prever o erro da média. O ajuste é aprendido só com anos anteriores.
- **Só em h1:** foi o único horizonte em que a correção reduziu o erro em 2005–2016, resultado confirmado em 2017 em diante.

## Métricas de teste

O viés é a média de real menos previsto: positivo significa que a previsão ficou abaixo do real.

| Horizonte | Previsões | MAE: previsão | MAE: Focus | RMSE: previsão | RMSE: Focus | Viés: previsão | Viés: Focus |
|---|---|---|---|---|---|---|---|
| Mês seguinte (h1) | 260 | **0,151** | 0,163 | **0,201** | 0,222 | −0,001 | +0,041 |
| 2 meses à frente (h2) | 259 | **0,206** | 0,211 | **0,283** | 0,294 | +0,021 | +0,051 |
| 3 meses à frente (h3) | 258 | **0,220** | 0,223 | **0,302** | 0,308 | +0,019 | +0,053 |

## Limites

- **Em relação ao Focus, é um empate:** além disso, o Focus é uma das partes da previsão, então os dois andam juntos.
- **De 2022 em diante e em 2017–2019,** o Focus teve erro médio de h1 a h3 menor. De 2022 em diante, ele também foi melhor em h1 (R² de 0,62, contra 0,56).
- **Pandemia:** foi o período mais difícil para os dois.
- **O R² cai rápido com a distância:** parte da inflação é choque imprevisível.
- **Previsão de h1 é quase do mês corrente:** a pesquisa usada já é do início do mês previsto.

## O que não entrou

- **Regressões e Random Forest:** usam a informação do histórico, com mais parâmetros, e erraram mais que a previsão oficial.
- **Selic e dólar:** colocá-los no modelo estatístico piorou a previsão, e o efeito dos juros não é mensurável com esses dados, porque o Banco Central sobe a Selic quando a inflação sobe.
- **Médias móveis e mesmo mês do ano anterior:** funcionam em alguns períodos, mas falham quando a inflação muda de patamar.
- **Pesos diferentes entre as partes da média:** não trouxeram ganho claro. Mesmo os melhores pesos possíveis param perto da média simples.

## Onde conferir

- Notebook `databricks/notebooks/treino_modelo_ipca.py` e experimento `previsao_ipca` no MLflow.
- Tabelas `gold.metricas_modelo` (erros), `gold.previsoes_ipca` (previsão oficial e previsto × realizado) e `gold.correcao_modelo`.
