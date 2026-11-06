# Previsão do IPCA com Machine Learning

**Objetivo:** prever o IPCA de cada um dos próximos 12 meses e o acumulado em 12 meses.

## O modelo

```
Previsão base = (ARIMA + Focus + Top 5) ÷ 3
Mês seguinte  = previsão base + correção pela revisão do Top 5
```

- **ARIMA:** aprende com o histórico do próprio IPCA.
- **Focus e Top 5:** expectativas de mercado publicadas pelo Banco Central.
- **Correção:** o mercado costuma reagir devagar às notícias, então a revisão das últimas 4 semanas ajuda a prever o erro da média.

**Exemplo, outubro/2026:** (0,61 + 0,27 + 0,21) ÷ 3 = 0,36; com a correção de −0,03, a previsão fica em **0,33%**.

## Como testamos

- Cada ano de 2005 a 2026 foi previsto só com dados anteriores, em 260 previsões para o mês seguinte.
- Nenhuma previsão usa pesquisa do futuro: com base em setembro, por exemplo, só entram as publicadas até 5 de outubro.

## Resultado

| | Erro médio h1–h3 (p.p.) | R² h1 | R² h2 | R² h3 | R² h12 |
|---|---|---|---|---|---|
| **Previsão** | **0,193** | **0,63** | **0,27** | **0,18** | **0,09** |
| Focus | 0,199 | 0,55 | 0,22 | 0,15 | 0,04 |

`h1`, `h2`, `h3` e `h12` são as previsões a 1, 2, 3 e 12 meses de distância. O R² mede quanto o modelo explica além da média do período.

A previsão tem números melhores que os do Focus no período todo, mas a diferença não é estatisticamente significativa: os dois ficam no mesmo nível.

## 2026, real × previsão × Focus (%)

| Mês | Real | Previsão | Focus |
|---|---|---|---|
| Janeiro | 0,33 | 0,35 | 0,36 |
| Fevereiro | 0,70 | 0,53 | 0,52 |
| Março | 0,88 | 0,36 | 0,32 |
| Abril | 0,67 | 0,62 | 0,48 |
| Maio | 0,58 | 0,48 | 0,40 |
| Junho | 0,16 | 0,35 | 0,30 |
| Julho | 0,07 | 0,33 | 0,30 |
| Agosto | −0,32 | −0,07 | −0,17 |
| Setembro | 0,82 | 0,40 | 0,53 |
| **Erro médio** | | **0,22** | **0,22** |

Previsões feitas um mês antes, com modelos treinados só até 2025.

## Previsão atual

Com o IPCA até setembro/2026 e o Focus de 30/10/2026.

| Mês | Real | Previsão | Faixa de 80% da previsão | Focus |
|---|---|---|---|---|
| Outubro/2026 | -- | 0,33% | 0,11% a 0,55% | 0,27% |
| Novembro/2026 | -- | 0,45% | 0,16% a 0,84% | 0,35% |
| Dezembro/2026 | -- | 0,59% | 0,28% a 1,00% | 0,60% |
| Acumulado até setembro/2027 | -- | 5,44% | 3,42% a 8,41% | 4,83% |

## O que não entrou

- **Regressões, Random Forest e médias móveis:** erraram mais que a previsão oficial.
- **Selic e dólar:** colocá-los no modelo estatístico piorou a previsão. O efeito dos juros não é mensurável com esses dados, porque o Banco Central sobe a Selic quando a inflação sobe.
- **Pesos diferentes na média:** não trouxeram ganho claro.

## Limites

- **Focus:** é um empate, e o Focus também é uma das partes da previsão. Ele foi melhor no mês seguinte de 2022 em diante (R² de 0,62, contra 0,56) e a 2 e 3 meses em 2017–2019.
- **Pandemia:** foi o período mais difícil. Em 2020–2021, o erro a 2 e 3 meses ficou perto de 0,40 p.p., mesmo sendo menor que o do Focus.
- **Mês seguinte:** é quase o mês corrente, porque a pesquisa usada já é do início do mês previsto.
- **Distância:** o R² cai rápido, porque parte da inflação é choque imprevisível.
