# Comparação dos modelos de previsão do IPCA

## Como os modelos foram testados

- **Objetivo:** prever o IPCA de cada um dos próximos 12 meses e o acumulado em 12 meses.
- **Validação ano a ano (2005 a 2026):** cada ano foi previsto por modelos treinados só com dados até o fim do ano anterior. São cerca de 250 previsões por horizonte.
- **Sem olhar para o futuro:** cada previsão usa só as pesquisas Focus publicadas até o dia 5 do mês seguinte ao mês base.
- **Métricas:** erro absoluto médio (MAE), em pontos percentuais, e R², que mede quanto o modelo explica além da média do período.

## Resultado

Erro médio de h1 a h3 e R² por horizonte. Entre parênteses, a diferença para o ARIMA.

| Modelo | Erro médio h1–h3 | R² h1 | R² h2 | R² h3 | R² h12 |
|---|---|---|---|---|---|
| **Média corrigida (modelo oficial)** | **0,193** (−0,036) | 0,64 (+0,34) | 0,28 (+0,16) | **0,18** (+0,14) | 0,09 (+0,09) |
| Média simples de ARIMA, Focus e Top 5 | 0,196 (−0,032) | 0,57 (+0,28) | 0,28 (+0,16) | **0,18** (+0,14) | 0,09 (+0,09) |
| Mercado: Top 5 | 0,195 (−0,033) | 0,61 (+0,32) | 0,24 (+0,12) | 0,16 (+0,12) | 0,05 (+0,05) |
| Mercado: Focus | 0,199 (−0,029) | 0,55 (+0,26) | 0,22 (+0,11) | 0,15 (+0,11) | 0,04 (+0,04) |
| **ARIMA** | 0,228 | 0,29 | 0,12 | 0,03 | 0,01 |
| Regressão múltipla (último IPCA + sazonal) | 0,239 (+0,011) | 0,27 (−0,02) | 0,10 (−0,02) | −0,01 (−0,05) | −0,17 (−0,18) |
| Ridge (8 variáveis) | 0,243 (+0,015) | 0,23 (−0,06) | 0,03 (−0,09) | −0,07 (−0,11) | −0,19 (−0,20) |
| Média móvel 12 meses | 0,244 (+0,016) | 0,05 (−0,24) | −0,02 (−0,14) | −0,07 (−0,10) | −0,37 (−0,37) |
| SARIMAX (juro real + dólar) | 0,252 (+0,024) | 0,21 (−0,08) | −0,04 (−0,15) | −0,22 (−0,25) | −0,31 (−0,32) |
| Média sazonal | 0,260 (+0,031) | 0,02 (−0,27) | 0,02 (−0,10) | 0,02 (−0,02) | 0,02 (+0,02) |
| Random Forest | 0,263 (+0,035) | 0,12 (−0,17) | −0,21 (−0,33) | −0,25 (−0,29) | −0,86 (−0,86) |
| Último mês | 0,283 (+0,055) | 0,07 (−0,22) | −0,31 (−0,42) | −0,64 (−0,67) | −0,48 (−0,48) |
| Mesmo mês do ano anterior | 0,300 (+0,072) | −0,51 (−0,80) | −0,51 (−0,62) | −0,51 (−0,54) | −0,48 (−0,48) |

No IPCA acumulado em 12 meses, o MAE é 1,45 p.p. no modelo oficial, 1,51 no Top 5, 1,52 no Focus e 1,74 no ARIMA.

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

## Métricas de teste do modelo oficial

| Horizonte | Previsões | MAE (p.p.) | RMSE (p.p.) | Viés (p.p.) | R² | Redução do MAE sobre o ARIMA |
|---|---|---|---|---|---|---|
| Mês seguinte (h1) | 260 | 0,151 | 0,201 | −0,001 | 0,64 | 26,7% |
| 2 meses à frente (h2) | 259 | 0,206 | 0,283 | +0,021 | 0,28 | 12,1% |
| 3 meses à frente (h3) | 258 | 0,220 | 0,302 | +0,019 | 0,18 | 9,6% |

## Limites

- **Em relação ao mercado, é um empate:** o modelo oficial tem números melhores no período todo, mas as diferenças não são estatisticamente significativas. De 2022 em diante, o mercado vai melhor em h1 (R² de 0,62 no Focus e 0,60 no Top 5, contra 0,56).
- **Em relação ao ARIMA, o ganho é claro,** exceto na pandemia (2020–2021), quando o ARIMA erra menos.
- **O R² cai rápido com a distância:** parte da inflação é choque imprevisível.
- **Previsão de h1 é quase do mês corrente:** a pesquisa usada já é do início do mês previsto.

## Por que não os outros

- **Regressões e Random Forest:** usam a mesma informação do ARIMA, com mais parâmetros, e erram mais.
- **SARIMAX:** piorou a previsão no geral, e o efeito dos juros não é mensurável com esses dados, porque o Banco Central sobe a Selic quando a inflação sobe.
- **Médias móveis e mesmo mês do ano anterior:** funcionam em alguns períodos, mas falham quando a inflação muda de patamar.
- **Pesos ajustados entre ARIMA, Focus e Top 5:** não trouxeram ganho claro. Mesmo os melhores pesos possíveis param perto da média simples.

## Onde conferir

- Notebook `databricks/notebooks/treino_modelo_ipca.py` e experimento `previsao_ipca` no MLflow.
- Tabelas `gold.metricas_modelo` (erros), `gold.previsoes_ipca` (previsão oficial e previsto × realizado) e `gold.correcao_modelo`.
- As regressões simples e o "mesmo mês do ano anterior" foram calculados à parte, com os mesmos meses de teste.
