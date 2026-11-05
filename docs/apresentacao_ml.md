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

Entre parênteses, a diferença para o ARIMA.

| Modelo | Erro médio h1–h3 (p.p.) | R² h1 | R² h2 | R² h3 | R² h12 |
|---|---|---|---|---|---|
| **Modelo oficial** | **0,193** (−0,036) | **0,64** (+0,34) | **0,28** (+0,16) | **0,18** (+0,14) | **0,09** (+0,09) |
| Top 5 | 0,195 (−0,033) | 0,61 (+0,32) | 0,24 (+0,12) | 0,16 (+0,12) | 0,05 (+0,05) |
| Focus | 0,199 (−0,029) | 0,55 (+0,26) | 0,22 (+0,11) | 0,15 (+0,11) | 0,04 (+0,04) |
| ARIMA | 0,228 | 0,29 | 0,12 | 0,03 | 0,01 |

`h1`, `h2`, `h3` e `h12` são as previsões a 1, 2, 3 e 12 meses de distância. O R² mede quanto o modelo explica além da média do período.

- **Contra o ARIMA:** erra menos, com ganho estatisticamente significativo de 1 a 6 meses à frente.
- **Contra o mercado:** fica no mesmo nível, sem diferença significativa.

## 2026, previsto × real (%)

| Mês | Real | Modelo oficial | ARIMA | Focus | Top 5 |
|---|---|---|---|---|---|
| Janeiro | 0,33 | 0,35 | 0,34 | 0,36 | 0,33 |
| Fevereiro | 0,70 | 0,53 | 0,59 | 0,52 | 0,50 |
| Março | 0,88 | 0,36 | 0,54 | 0,32 | 0,30 |
| Abril | 0,67 | 0,62 | 0,71 | 0,48 | 0,42 |
| Maio | 0,58 | 0,48 | 0,56 | 0,40 | 0,38 |
| Junho | 0,16 | 0,35 | 0,52 | 0,30 | 0,21 |
| Julho | 0,07 | 0,33 | 0,28 | 0,30 | 0,31 |
| Agosto | −0,32 | −0,07 | 0,15 | −0,17 | −0,12 |
| Setembro | 0,82 | 0,40 | 0,07 | 0,53 | 0,59 |
| **Erro médio** | | **0,22** | **0,26** | **0,22** | **0,22** |

Previsões feitas um mês antes, com modelos treinados só até 2025.

## Previsão atual

Com o IPCA até setembro/2026 e o Focus de 30/10/2026.

| Mês | Previsão | Faixa de 80% |
|---|---|---|
| Outubro/2026 | 0,33% | 0,11% a 0,55% |
| Novembro/2026 | 0,45% | 0,16% a 0,84% |
| Dezembro/2026 | 0,59% | 0,28% a 1,00% |
| Acumulado até setembro/2027 | 5,44% | 3,42% a 8,41% |

## O que não entrou

- **Regressões, Random Forest e médias móveis:** erraram mais que o ARIMA.
- **Selic e dólar (SARIMAX):** pioraram a previsão. O efeito dos juros não é mensurável com esses dados, porque o Banco Central sobe a Selic quando a inflação sobe.
- **Pesos diferentes na média:** dar mais peso ao ARIMA piorou o resultado.

## Limites

- **Mercado:** é um empate. De 2022 em diante, o mercado foi melhor no mês seguinte (R² de 0,62 no Focus e 0,60 no Top 5, contra 0,56).
- **Pandemia:** em 2020–2021, o ARIMA errou menos.
- **Mês seguinte:** é quase o mês corrente, porque a pesquisa usada já é do início do mês previsto.
- **Distância:** o R² cai rápido, porque parte da inflação é choque imprevisível.
