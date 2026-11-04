# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Treino do modelo de previsão do IPCA
# MAGIC
# MAGIC Treina um modelo por horizonte (estratégia direta) para prever o IPCA de cada um dos próximos 12 meses (`h1` a `h12`) e o IPCA acumulado em 12 meses (`acum_12m`), a partir da tabela `projeto_hands_on.gold.features_mensal`.
# MAGIC
# MAGIC | Modelo | Tipo | Papel |
# MAGIC |---|---|---|
# MAGIC | `ultimo_mes` | Referência | Repete o IPCA do mês base (ou o acumulado em 12 meses) |
# MAGIC | `media_movel_3m`, `_6m`, `_12m` | Referência | Média do IPCA dos últimos 3, 6 ou 12 meses, repetida para os meses seguintes |
# MAGIC | `sazonal` | Referência | Média histórica do mês do calendário previsto |
# MAGIC | `arima` | Série temporal | SARIMA ajustado só no histórico do IPCA, com a ordem escolhida por AIC antes do primeiro ano de teste |
# MAGIC | `sarimax`, `sarimax_ampliado` | Série temporal | O mesmo SARIMA com variáveis externas do mês anterior: juro real e dólar (e, no ampliado, variação da Selic e do dólar em 3 meses). Nas previsões, elas ficam constantes no último valor conhecido, como num cenário do simulador |
# MAGIC | `ridge` | Regressão | Regressão linear com regularização, para comparação; os coeficientes vão para `gold.coeficientes_modelo` |
# MAGIC | `random_forest` | Regressão | Comparação não linear com todas as features |
# MAGIC | `focus`, `top5` | Mercado | Mediana das expectativas do Boletim Focus e do ranking Top 5 de curto prazo, só com as pesquisas publicadas até o dia 5 do mês seguinte ao mês base |
# MAGIC | `media_tres` | Combinação | Média simples de ARIMA, Focus e Top 5 |
# MAGIC | `media_tres_corrigida` | Combinação | **Modelo oficial**: a média simples, com uma correção em `h1` proporcional à revisão do Top 5 nas últimas 4 semanas (o mercado costuma reagir devagar às notícias) |
# MAGIC
# MAGIC **Validação (origem móvel anual):** para cada ano de teste, de 2005 até o ano atual, os modelos são treinados só com meses cujo alvo é anterior a esse ano e testados nos meses base do ano. O erro é resumido por período: 2005–2016, 2017–2019 (inflação baixa e estável), 2020–2021 (pandemia) e 2022 em diante. Métricas e parâmetros ficam registrados no MLflow.
# MAGIC
# MAGIC **Previsão atual:** os modelos são reajustados com todo o histórico e aplicados ao mês base mais recente, com a pesquisa Focus mais recente disponível. A previsão oficial está em `previsao_oficial`, e a faixa de incerteza (80%) vem dos erros dela em todos os anos de teste.
# MAGIC
# MAGIC Tabelas gravadas: `gold.previsoes_ipca`, `gold.metricas_modelo`, `gold.correcao_modelo`, `gold.parametros_arima` e `gold.coeficientes_modelo`.

# COMMAND ----------

# DBTITLE 1,Dependencies
# MAGIC %pip install statsmodels==0.15.0 --quiet

# COMMAND ----------

# DBTITLE 1,Parameters
# Origem e destino; o teste em sandbox aponta para outras tabelas e usa um prefixo para não tocar na gold
dbutils.widgets.text("origem", "projeto_hands_on.gold.features_mensal", "Tabela de features")
dbutils.widgets.text("expectativas", "projeto_hands_on.silver.focus_ipca", "Tabela de expectativas do Focus")
dbutils.widgets.text("destino", "projeto_hands_on.gold", "Catálogo.schema de destino")
dbutils.widgets.text("prefixo_tabela", "", "Prefixo das tabelas de saída")
# "sim" retreina mesmo sem IPCA novo (por exemplo, depois de mudar o código do modelo)
dbutils.widgets.dropdown("forcar_treino", "nao", ["nao", "sim"], "Retreinar sem IPCA novo")

# COMMAND ----------

# DBTITLE 1,Environment
import os
import warnings
from datetime import datetime, timezone

import mlflow
import numpy as np
import pandas as pd
from pyspark.sql.functions import col, date_trunc, isnan, max_by, when
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge, RidgeCV
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.statespace.sarimax import SARIMAX

ORIGEM = dbutils.widgets.get("origem").strip()
FOCUS_SILVER = dbutils.widgets.get("expectativas").strip()
DESTINO = dbutils.widgets.get("destino").strip()
PREFIXO = dbutils.widgets.get("prefixo_tabela").strip()
FORCAR_TREINO = dbutils.widgets.get("forcar_treino") == "sim"
IPCA_SILVER = "projeto_hands_on.silver.ipca"
SELIC_SILVER = "projeto_hands_on.silver.selic"
MOEDAS_SILVER = "projeto_hands_on.silver.moedas_conversao"

# Primeiro ano de teste: a tabela de features começa em 2001, então o primeiro treino tem 4 anos
PRIMEIRO_ANO_TESTE = 2005
# Histórico do IPCA para o ARIMA e as médias móveis: depois da estabilização do Plano Real
INICIO_HISTORICO_IPCA = "1997-01-01"
# Histórico do SARIMAX: a Selic meta começa em 03/1999, e o juro real usa a Selic do mês anterior
INICIO_SARIMAX = "2000-01-01"
# Períodos de leitura dos resultados, pelo mês previsto
PERIODOS = {
    "2005-2016": ("2005-01-01", "2016-12-01"),
    "2017-2019": ("2017-01-01", "2019-12-01"),
    "2020-2021": ("2020-01-01", "2021-12-01"),
    "2022 em diante": ("2022-01-01", "2099-12-01"),
}
TODOS = "2005 em diante"

HORIZONTES = list(range(1, 13))
# Nome do alvo -> coluna da features_mensal
ALVOS = {**{f"h{h}": f"alvo_ipca_h{h}" for h in HORIZONTES}, "acum_12m": "alvo_ipca_acum_12m"}
MEDIAS_MOVEIS = [3, 6, 12]
# Variáveis externas de cada SARIMAX, sempre do mês anterior ao IPCA explicado (mesmas definições da features_mensal)
EXOGENAS_SARIMAX = {
    "sarimax": ["juro_real_12m", "usd_var_12m"],
    "sarimax_ampliado": ["juro_real_12m", "selic_var_12m_pp", "usd_var_3m", "usd_var_12m"],
}
SERIES_TEMPORAIS = ["arima", *EXOGENAS_SARIMAX]
# Modelos treinados só com os dados do projeto
MODELOS_BASE = ["ultimo_mes", "media_movel_3m", "media_movel_6m", "media_movel_12m", "sazonal", *SERIES_TEMPORAIS,
                "ridge", "random_forest"]
MODELOS = [*MODELOS_BASE, "focus", "top5", "media_tres", "media_tres_corrigida"]

# Features do Ridge: poucas, interpretáveis e calculáveis a partir dos parâmetros do simulador (Selic e dólar).
# "ipca_sazonal" é a média histórica do mês previsto (ipca_sazonal_hN do horizonte, ou a composta dos 12 meses).
FEATURES_LINEAR = [
    "ipca_lag1",
    "ipca_acum_3m",
    "ipca_12m_menos_meta",
    "ipca_sazonal",
    "juro_real_12m",
    "selic_var_12m_pp",
    "usd_var_3m",
    "usd_var_12m",
]
# O Random Forest recebe também as demais features (lags, níveis, volatilidade e comparações em z-score)
FEATURES_RF_EXTRAS = [
    "ipca_lag2",
    "ipca_lag3",
    "ipca_lag12",
    "ipca_acum_12m",
    "ipca_menos_sazonal_lag1",
    "meta_inflacao",
    "selic_lag1",
    "selic_var_3m_pp",
    "meses_desde_mudanca_selic",
    "usd_lag1",
    "usd_var_lag1",
    "usd_volatilidade_lag1",
    "usd_menos_ipca_12m",
    "usd_vs_selic_z",
    "ipca_vs_selic_z",
    "usd_vs_ipca_z",
]
FEATURES_RF = FEATURES_LINEAR + FEATURES_RF_EXTRAS
ALPHAS_RIDGE = np.logspace(-2, 3, 30).tolist()
PARAMS_RF = {"n_estimators": 300, "min_samples_leaf": 3, "random_state": 42, "n_jobs": -1}
# Ordens candidatas do ARIMA: (p, d, q) e sazonal (P, D, Q, 12). O IPCA mensal é estacionário no período, então d = 0
ORDENS_ARIMA = [(p, 0, q) for p in range(3) for q in range(3)]
SAZONAIS_ARIMA = [(0, 0, 0, 12), (1, 0, 0, 12), (0, 0, 1, 12), (1, 0, 1, 12)]
# Modelo da previsão oficial: média simples de ARIMA, Focus e Top 5, com correção pela revisão do Top 5 em h1.
# Os horizontes corrigidos foram os que reduziram o erro em 2005-2016, confirmados depois em 2017 em diante
MODELO_OFICIAL = "media_tres_corrigida"
ALVOS_CORRIGIDOS = ["h1"]
ALPHA_CORRECAO = 50.0
# O histórico da correção começa em 2002: o ARIMA precisa de alguns anos de treino, e o Top 5 começa em 11/2001
PRIMEIRO_ANO_CORRECAO = 2002
JANELA_REVISAO_DIAS = 28
MAXIMO_DIAS_PESQUISA = 45
# Faixa de incerteza de 80%: quantis 10% e 90% dos erros do modelo nos testes
QUANTIS_FAIXA = (0.10, 0.90)

# COMMAND ----------

# DBTITLE 1,Functions
def meses_a_frente(alvo):
    """Months between the base month and the (last) forecast month: h3 -> 3, acum_12m -> 12."""
    return 12 if alvo == "acum_12m" else int(alvo[1:])


def compor(mensais):
    """Compounds monthly % changes into the accumulated % change (last axis)."""
    return (np.prod(1 + np.asarray(mensais, dtype=float) / 100, axis=-1) - 1) * 100


def preparar(df, alvo):
    """Adds the horizon-specific seasonal column 'ipca_sazonal' and the target column 'y'."""
    dados = df.copy()
    if alvo == "acum_12m":
        dados["ipca_sazonal"] = compor(np.column_stack([dados[f"ipca_sazonal_h{h}"] for h in HORIZONTES]))
    else:
        dados["ipca_sazonal"] = dados[f"ipca_sazonal_h{meses_a_frente(alvo)}"]
    dados["y"] = dados[ALVOS[alvo]]
    return dados


def separar_por_ano(dados, alvo, ano):
    """Walk-forward split. Train: rows whose (last) target month is before January of `ano`.
    Test: base months inside `ano` with a known target."""
    com_alvo = dados[dados["y"].notna()]
    inicio = pd.Timestamp(year=ano, month=1, day=1)
    mes_alvo = com_alvo["mes_base"] + pd.DateOffset(months=meses_a_frente(alvo))
    return com_alvo[mes_alvo < inicio], com_alvo[com_alvo["mes_base"].dt.year == ano]


def medias_moveis(ipca, janelas):
    """Mean of the last k monthly IPCA values up to each month (inclusive), one column per window."""
    return pd.DataFrame({f"media_movel_{k}m": ipca.rolling(k).mean() for k in janelas})


def previsao_referencia(dados, alvo, modelo):
    """Baseline forecasts from columns already in `dados` (moving averages added by the caller)."""
    if modelo == "sazonal":
        return dados["ipca_sazonal"]
    if modelo == "ultimo_mes":
        return dados["ipca_acum_12m"] if alvo == "acum_12m" else dados["ipca_lag1"]
    # Média móvel: o mesmo valor mensal para todos os meses à frente
    mensal = dados[modelo]
    return pd.Series(compor(np.column_stack([mensal] * 12)), index=dados.index) if alvo == "acum_12m" else mensal


def periodo_de(mes_previsto):
    for nome, (inicio, fim) in PERIODOS.items():
        if pd.Timestamp(inicio) <= mes_previsto <= pd.Timestamp(fim):
            return nome
    return None


def exogenas_mensais(ipca, selic, usd):
    """Monthly external variables, aligned with the gold.features_mensal definitions, indexed by month."""
    acum_12m = (np.exp(np.log1p(ipca / 100).rolling(12).sum()) - 1) * 100
    return pd.DataFrame({
        "juro_real_12m": selic - acum_12m,
        "selic_var_12m_pp": selic - selic.shift(12),
        "usd_var_3m": (usd / usd.shift(3) - 1) * 100,
        "usd_var_12m": (usd / usd.shift(12) - 1) * 100,
    })


def ajustar_arima(serie, ordem, sazonal, exog=None):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return SARIMAX(serie, exog=exog, order=ordem, seasonal_order=sazonal, trend="c").fit(disp=False)


def escolher_ordem_arima(serie):
    """Picks the (order, seasonal order) with the lowest AIC on the given history."""
    candidatos = [(ajustar_arima(serie, o, s).aic, o, s) for o in ORDENS_ARIMA for s in SAZONAIS_ARIMA]
    _, ordem, sazonal = min(candidatos, key=lambda c: c[0])
    return ordem, sazonal


def alinhar_exogenas(serie, exog):
    """Pairs the IPCA of month t with exog of t-1, keeping only months where every variable exists."""
    defasadas = exog.shift(1).reindex(serie.index)
    serie = serie[defasadas.notna().all(axis=1)]
    return serie, defasadas.loc[serie.index]


def previsoes_arima(serie, ordem, sazonal, fim_treino, origens, exog=None):
    """Fits the (S)ARIMA(X) on the series up to fim_treino and, keeping those parameters, forecasts the next 12
    months from each origin using only the data up to that origin. Returns {origin: array of 12 forecasts}.

    exog (optional) holds the external variables by month; the IPCA of month t is explained by exog of t-1,
    and in the forecast the last known values stay constant, as in a simulator scenario.
    """
    defasadas = None
    if exog is not None:
        serie, defasadas = alinhar_exogenas(serie, exog)

    def ate(dados, mes):
        return None if dados is None else dados[dados.index <= mes]

    ajuste = ajustar_arima(ate(serie, fim_treino), ordem, sazonal, ate(defasadas, fim_treino))
    previsoes = {}
    for origem in origens:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            filtrado = ajuste.apply(ate(serie, origem), exog=ate(defasadas, origem))
            futuras = None if exog is None else np.tile(exog.loc[origem].to_numpy(dtype=float), (12, 1))
            previsoes[origem] = np.asarray(filtrado.forecast(12, exog=futuras), dtype=float)
    return previsoes


def parametros_sarimax(ajuste):
    """Coefficients of a fitted SARIMAX with standard error, p-value and 95% interval."""
    intervalo = ajuste.conf_int()
    return pd.DataFrame({
        "parametro": ajuste.params.index,
        "coeficiente": ajuste.params.to_numpy(dtype=float),
        "erro_padrao": ajuste.bse.to_numpy(dtype=float),
        "p_valor": ajuste.pvalues.to_numpy(dtype=float),
        "ic95_inferior": intervalo.iloc[:, 0].to_numpy(dtype=float),
        "ic95_superior": intervalo.iloc[:, 1].to_numpy(dtype=float),
    })


def previsao_arima_alvo(previsoes, origem, alvo):
    valores = previsoes[origem]
    return float(compor(valores)) if alvo == "acum_12m" else float(valores[meses_a_frente(alvo) - 1])


def indexar_expectativas(df):
    """Groups the surveys of one source by reference month, sorted by survey date."""
    return {ref: g.sort_values("data_pesquisa") for ref, g in df.groupby("mes_referencia")}


def expectativa_em(indice, referencia, limite, maximo_dias=MAXIMO_DIAS_PESQUISA):
    """Median expectation for the reference month in the last survey published up to `limite`."""
    g = indice.get(referencia)
    if g is None:
        return np.nan
    i = g["data_pesquisa"].searchsorted(limite, side="right") - 1
    if i < 0 or (limite - g["data_pesquisa"].iloc[i]).days > maximo_dias:
        return np.nan
    return float(g["mediana"].iloc[i])


def limite_pesquisa(mes_base):
    """Day 5 of the month after the base month: the last survey date a test forecast may use."""
    return mes_base + pd.DateOffset(months=1, days=4)


def linhas_mercado(mes_base, arima, indice_focus, indice_top5, ipca, limite):
    """One row per target with ARIMA, Focus, Top 5 and the 4-week Top 5 revision known at `limite`."""
    meses = [mes_base + pd.DateOffset(months=h) for h in HORIZONTES]
    antes = limite - pd.Timedelta(days=JANELA_REVISAO_DIAS)
    focus = [expectativa_em(indice_focus, m, limite) for m in meses]
    top5_bruto = [expectativa_em(indice_top5, m, limite) for m in meses]
    top5_antes = [expectativa_em(indice_top5, m, antes) for m in meses]
    # Na série de 2002 a 2004 o Top 5 não cobre todos os horizontes: nesses casos vale o Focus
    top5 = [f if np.isnan(t) else t for t, f in zip(top5_bruto, focus)]
    reais = [ipca.get(m, np.nan) for m in meses]
    linhas = [
        {"alvo": f"h{h}", "mes_base": mes_base, "mes_previsto": meses[h - 1], "y": reais[h - 1], "arima": float(arima[h - 1]),
         "focus": focus[h - 1], "top5": top5[h - 1], "revisao_top5": top5_bruto[h - 1] - top5_antes[h - 1]}
        for h in HORIZONTES
    ]
    linhas.append({
        "alvo": "acum_12m", "mes_base": mes_base, "mes_previsto": meses[-1],
        "y": float(compor(reais)) if not np.isnan(reais).any() else np.nan, "arima": float(compor(arima)),
        "focus": float(compor(focus)), "top5": float(compor(top5)), "revisao_top5": np.nan,
    })
    return linhas


def nova_correcao():
    return make_pipeline(StandardScaler(), Ridge(alpha=ALPHA_CORRECAO))


def ajustar_correcao(historico):
    """Fits the error of the simple average on the Top 5 revision, using rows with a known target and revision."""
    base = historico[historico["y"].notna() & historico["revisao_top5"].notna()]
    return nova_correcao().fit(base[["revisao_top5"]], base["y"] - base["media_tres"]), base


def media_corrigida(painel, anos):
    """Simple average plus, on the corrected targets, the correction fitted only with months before each year."""
    resultado = painel["media_tres"].copy()
    for alvo in ALVOS_CORRIGIDOS:
        do_alvo = painel[painel["alvo"] == alvo]
        for ano in anos:
            passado = do_alvo[do_alvo["mes_previsto"] < pd.Timestamp(year=ano, month=1, day=1)]
            teste = do_alvo[do_alvo["mes_base"].dt.year == ano]
            if teste.empty:
                continue
            modelo, base = ajustar_correcao(passado)
            revisao = teste[["revisao_top5"]].fillna(base["revisao_top5"].mean())
            resultado.loc[teste.index] = teste["media_tres"] + modelo.predict(revisao)
    return resultado


def novo_ridge():
    return make_pipeline(StandardScaler(), RidgeCV(alphas=ALPHAS_RIDGE, cv=TimeSeriesSplit(n_splits=5)))


def coeficientes_originais(modelo, features):
    """Converts a StandardScaler + Ridge pipeline into intercept and coefficients in the original units.

    previsao = intercepto + soma(coeficiente * valor), which is what the SQL simulator evaluates.
    """
    escala, ridge = modelo.steps[0][1], modelo.steps[-1][1]
    coeficientes = ridge.coef_ / escala.scale_
    intercepto = ridge.intercept_ - float(np.sum(coeficientes * escala.mean_))
    return float(intercepto), dict(zip(features, coeficientes.astype(float)))


def faixa_erro(reais, previstos, quantis):
    """Quantiles of the errors (real - forecast), added to a forecast to build the uncertainty band."""
    erros = np.asarray(reais, dtype=float) - np.asarray(previstos, dtype=float)
    return tuple(float(np.quantile(erros, q)) for q in quantis)


def metricas(reais, previstos):
    erros = np.asarray(reais, dtype=float) - np.asarray(previstos, dtype=float)
    return {"mae": float(np.mean(np.abs(erros))), "rmse": float(np.sqrt(np.mean(erros**2))), "n_teste": len(erros)}


# COMMAND ----------

# DBTITLE 1,Load Data
features = spark.read.table(ORIGEM).toPandas()
features["mes_base"] = pd.to_datetime(features["mes_base"])
features = features.sort_values("mes_base").reset_index(drop=True)

ipca = (
    spark.read.table(IPCA_SILVER)
    .filter(col("data_ref") >= INICIO_HISTORICO_IPCA)
    .select("data_ref", col("valor").cast("double").alias("ipca"))
    .toPandas()
)
serie_ipca = ipca.set_index(pd.to_datetime(ipca["data_ref"]))["ipca"].sort_index().asfreq("MS")
assert serie_ipca.notna().all(), "Há meses sem IPCA no histórico"

features = features.join(medias_moveis(serie_ipca, MEDIAS_MOVEIS), on="mes_base")


def mensal(pdf, coluna):
    return pdf.set_index(pd.to_datetime(pdf["data_ref"]))[coluna].sort_index().reindex(serie_ipca.index)


selic = spark.read.table(SELIC_SILVER).select("data_ref", col("valor").cast("double").alias("selic")).toPandas()
usd = (
    spark.read.table(MOEDAS_SILVER)
    .filter(col("moeda") == "USD")
    .groupBy(date_trunc("month", "data_cotacao").cast("date").alias("data_ref"))
    .agg(max_by(col("valor_brl").cast("double"), "data_cotacao").alias("usd"))
    .toPandas()
)
exogenas = exogenas_mensais(serie_ipca, mensal(selic, "selic"), mensal(usd, "usd"))
serie_sarimax = serie_ipca[serie_ipca.index >= INICIO_SARIMAX]

linha_atual = features[features["conjunto"] == "previsao"]
assert len(linha_atual) == 1, f"Esperada uma linha de previsão em {ORIGEM}, encontradas {len(linha_atual)}"
assert features["features_completas"].all(), "Há meses base com features nulas na tabela de origem"
mes_base_atual = linha_atual["mes_base"].iloc[0]
assert serie_ipca.index[-1] == mes_base_atual, "O último IPCA da silver não é o mês base mais recente das features"
anos_teste = list(range(PRIMEIRO_ANO_TESTE, mes_base_atual.year + 1))
print(f"{len(features)} meses base, de {features['mes_base'].min():%m/%Y} a {mes_base_atual:%m/%Y}")
print(f"Anos de teste: {anos_teste[0]} a {anos_teste[-1]}; previsão a partir de {mes_base_atual:%m/%Y}")

# COMMAND ----------

# DBTITLE 1,Skip When There Is No New IPCA
# A validação leva mais de 10 minutos e só muda quando sai um IPCA novo (uma vez por mês). Nos outros dias o Job
# termina aqui, mantendo as tabelas do último treino.
tabela_previsoes = f"{DESTINO}.{PREFIXO}previsoes_ipca"
if not FORCAR_TREINO and spark.catalog.tableExists(tabela_previsoes):
    ultimo = (
        spark.read.table(tabela_previsoes)
        .filter(col("tipo") == "previsao")
        .agg({"mes_base": "max"})
        .first()[0]
    )
    if ultimo is not None and pd.Timestamp(ultimo) == mes_base_atual:
        dbutils.notebook.exit(f"Sem IPCA novo desde o último treino (mês base {mes_base_atual:%m/%Y}); nada a fazer")

# COMMAND ----------

# DBTITLE 1,Walk-Forward Backtest
usuario = spark.sql("SELECT current_user()").first()[0]
# No serverless, o MLflow tenta ler o registry da configuração do Spark (bloqueada) se ele não estiver definido
os.environ.setdefault("MLFLOW_REGISTRY_URI", "databricks-uc")
mlflow.set_experiment(f"/Users/{usuario}/previsao_ipca")
agora = datetime.now(timezone.utc).replace(tzinfo=None)
run = mlflow.start_run(run_name=f"treino_{agora:%Y%m%d_%H%M}")

# Ordem do ARIMA escolhida só com o histórico anterior ao primeiro ano de teste
ordem_arima, sazonal_arima = escolher_ordem_arima(
    serie_ipca[serie_ipca.index < pd.Timestamp(year=PRIMEIRO_ANO_TESTE, month=1, day=1)]
)
print(f"ARIMA escolhido: order={ordem_arima}, seasonal_order={sazonal_arima}")

preparados = {alvo: preparar(features, alvo) for alvo in ALVOS}
linhas_teste = []
arima_por_origem = {}

for ano in anos_teste:
    fim_treino = pd.Timestamp(year=ano - 1, month=12, day=1)
    origens = list(features.loc[features["mes_base"].dt.year == ano, "mes_base"])
    temporais = {"arima": previsoes_arima(serie_ipca, ordem_arima, sazonal_arima, fim_treino, origens)}
    arima_por_origem.update(temporais["arima"])
    for modelo, colunas in EXOGENAS_SARIMAX.items():
        temporais[modelo] = previsoes_arima(serie_sarimax, ordem_arima, sazonal_arima, fim_treino, origens,
                                            exog=exogenas[colunas])

    for alvo, dados in preparados.items():
        treino, teste = separar_por_ano(dados, alvo, ano)
        if teste.empty:
            continue
        ridge = novo_ridge().fit(treino[FEATURES_LINEAR], treino["y"])
        rf = RandomForestRegressor(**PARAMS_RF).fit(treino[FEATURES_RF], treino["y"])
        previstos = {modelo: np.asarray(previsao_referencia(teste, alvo, modelo), dtype=float)
                     for modelo in MODELOS_BASE if modelo not in (*SERIES_TEMPORAIS, "ridge", "random_forest")}
        for modelo, previsoes in temporais.items():
            previstos[modelo] = np.array([previsao_arima_alvo(previsoes, o, alvo) for o in teste["mes_base"]])
        previstos["ridge"] = ridge.predict(teste[FEATURES_LINEAR])
        previstos["random_forest"] = rf.predict(teste[FEATURES_RF])

        for i, (_, linha) in enumerate(teste.iterrows()):
            mes_previsto = linha["mes_base"] + pd.DateOffset(months=meses_a_frente(alvo))
            linhas_teste.append({
                "tipo": "teste",
                "mes_base": linha["mes_base"],
                "alvo": alvo,
                "mes_previsto": mes_previsto,
                "periodo": periodo_de(mes_previsto),
                "valor_real": float(linha["y"]),
                **{f"previsao_{modelo}": float(previstos[modelo][i]) for modelo in MODELOS_BASE},
            })
    print(f"{ano}: {len(origens)} meses base testados")

backtest = pd.DataFrame(linhas_teste)

# COMMAND ----------

# DBTITLE 1,Market Expectations and Official Model
expectativas = spark.read.table(FOCUS_SILVER).toPandas()
for coluna in ("data_pesquisa", "mes_referencia"):
    expectativas[coluna] = pd.to_datetime(expectativas[coluna])
indice_focus = indexar_expectativas(expectativas[expectativas["fonte"] == "focus"])
indice_top5 = indexar_expectativas(expectativas[expectativas["fonte"] == "top5"])

# ARIMA dos anos anteriores ao primeiro teste, só para formar o histórico de erros que treina a correção
for ano in range(PRIMEIRO_ANO_CORRECAO, PRIMEIRO_ANO_TESTE):
    origens = list(serie_ipca.index[serie_ipca.index.year == ano])
    fim_treino = pd.Timestamp(year=ano - 1, month=12, day=1)
    arima_por_origem.update(previsoes_arima(serie_ipca, ordem_arima, sazonal_arima, fim_treino, origens))

painel = pd.DataFrame([
    linha for origem in sorted(arima_por_origem)
    for linha in linhas_mercado(origem, arima_por_origem[origem], indice_focus, indice_top5, serie_ipca, limite_pesquisa(origem))
])
painel["media_tres"] = painel[["arima", "focus", "top5"]].mean(axis=1, skipna=False)
painel["media_tres_corrigida"] = media_corrigida(painel, anos_teste)

colunas_mercado = {"focus": "previsao_focus", "top5": "previsao_top5", "media_tres": "previsao_media_tres",
                   "media_tres_corrigida": "previsao_media_tres_corrigida"}
backtest = backtest.merge(painel[["mes_base", "alvo", *colunas_mercado]].rename(columns=colunas_mercado),
                          on=["mes_base", "alvo"], how="left", validate="one_to_one")
assert backtest[list(colunas_mercado.values())].notna().all().all(), "Há previsões de teste sem expectativa do Focus"
backtest["previsao_oficial"] = backtest[f"previsao_{MODELO_OFICIAL}"]

# COMMAND ----------

# DBTITLE 1,Metrics by Period
linhas_metricas = []
for alvo in ALVOS:
    do_alvo = backtest[backtest["alvo"] == alvo]
    grupos = [(TODOS, do_alvo)] + [(p, do_alvo[do_alvo["periodo"] == p]) for p in PERIODOS]
    for periodo, grupo in grupos:
        if grupo.empty:
            continue
        for modelo in MODELOS:
            m = metricas(grupo["valor_real"], grupo[f"previsao_{modelo}"])
            linhas_metricas.append({"alvo": alvo, "horizonte": meses_a_frente(alvo), "modelo": modelo,
                                    "periodo": periodo, **m})
tabela_metricas = pd.DataFrame(linhas_metricas)

# MAE médio de h1 a h12 por período (p.p.): quanto menor, melhor
mensais = tabela_metricas[tabela_metricas["alvo"] != "acum_12m"]
resumo = mensais.pivot_table(index="modelo", columns="periodo", values="mae", aggfunc="mean")
resumo = resumo[[TODOS, *PERIODOS]].loc[MODELOS].round(3)
display(resumo.reset_index())
acumulado = tabela_metricas[tabela_metricas["alvo"] == "acum_12m"].pivot(index="modelo", columns="periodo", values="mae")
display(acumulado[[TODOS, *PERIODOS]].loc[MODELOS].round(3).reset_index())

mlflow.log_params({
    "primeiro_ano_teste": PRIMEIRO_ANO_TESTE,
    "modelo_oficial": MODELO_OFICIAL,
    "mes_base_previsao": f"{mes_base_atual:%Y-%m}",
    "arima_order": str(ordem_arima),
    "arima_seasonal_order": str(sazonal_arima),
    **{f"exogenas_{m}": ",".join(c) for m, c in EXOGENAS_SARIMAX.items()},
    "features_linear": ",".join(FEATURES_LINEAR),
    "features_rf": ",".join(FEATURES_RF),
    "alphas_ridge": f"{ALPHAS_RIDGE[0]:g} a {ALPHAS_RIDGE[-1]:g} ({len(ALPHAS_RIDGE)} valores)",
    **{f"rf_{k}": v for k, v in PARAMS_RF.items()},
})
total = tabela_metricas[tabela_metricas["periodo"] == TODOS]
mlflow.log_metrics({f"mae_{r.modelo}_{r.alvo}": r.mae for r in total.itertuples()})
mlflow.log_metrics({
    f"mae_medio_h1_h12_{modelo}_{periodo.replace(' ', '_')}": float(valor)
    for periodo in resumo.columns for modelo, valor in resumo[periodo].items()
})
mlflow.log_text(tabela_metricas.to_csv(index=False), "metricas.csv")

# COMMAND ----------

# DBTITLE 1,Final Models and Current Forecast
temporais_final = {"arima": previsoes_arima(serie_ipca, ordem_arima, sazonal_arima, mes_base_atual, [mes_base_atual])}
# Previsão atual: usa a pesquisa mais recente disponível no dia do treino
mercado_atual = pd.DataFrame(linhas_mercado(mes_base_atual, temporais_final["arima"][mes_base_atual], indice_focus, indice_top5,
                                            serie_ipca, pd.Timestamp(agora.date()))).set_index("alvo")
mercado_atual["media_tres"] = mercado_atual[["arima", "focus", "top5"]].mean(axis=1, skipna=False)
assert mercado_atual["media_tres"].notna().all(), f"Sem pesquisa recente do Focus em {FOCUS_SILVER}"
mercado_atual["media_tres_corrigida"] = mercado_atual["media_tres"]
linhas_correcao = []
for alvo in ALVOS_CORRIGIDOS:
    modelo_correcao, base_correcao = ajustar_correcao(painel[painel["alvo"] == alvo])
    intercepto_correcao, coeficientes_correcao = coeficientes_originais(modelo_correcao, ["revisao_top5"])
    revisao_atual = mercado_atual.loc[alvo, "revisao_top5"]
    revisao_usada = base_correcao["revisao_top5"].mean() if np.isnan(revisao_atual) else revisao_atual
    ajuste_atual = intercepto_correcao + coeficientes_correcao["revisao_top5"] * revisao_usada
    mercado_atual.loc[alvo, "media_tres_corrigida"] = mercado_atual.loc[alvo, "media_tres"] + ajuste_atual
    linhas_correcao.append({"alvo": alvo, "intercepto": intercepto_correcao, "coeficiente_revisao": coeficientes_correcao["revisao_top5"],
                            "alpha": ALPHA_CORRECAO, "n_treino": len(base_correcao), "revisao_atual": revisao_usada,
                            "ajuste_atual": ajuste_atual})
tabela_correcao = pd.DataFrame(linhas_correcao)
display(tabela_correcao.round(4))

parametros = [
    parametros_sarimax(ajustar_arima(serie_ipca, ordem_arima, sazonal_arima)).assign(modelo="arima",
                                                                                    n_meses=len(serie_ipca))
]
for modelo, colunas in EXOGENAS_SARIMAX.items():
    temporais_final[modelo] = previsoes_arima(serie_sarimax, ordem_arima, sazonal_arima, mes_base_atual,
                                              [mes_base_atual], exog=exogenas[colunas])
    endog, defasadas = alinhar_exogenas(serie_sarimax, exogenas[colunas])
    ajuste = ajustar_arima(endog, ordem_arima, sazonal_arima, defasadas)
    parametros.append(parametros_sarimax(ajuste).assign(modelo=modelo, n_meses=len(endog)))
tabela_parametros = pd.concat(parametros, ignore_index=True)
display(tabela_parametros.round(4))
linhas_coeficientes, linhas_previsao, importancias = [], [], []

for alvo, dados in preparados.items():
    com_alvo = dados[dados["y"].notna()]
    ridge_final = novo_ridge().fit(com_alvo[FEATURES_LINEAR], com_alvo["y"])
    rf_final = RandomForestRegressor(**PARAMS_RF).fit(com_alvo[FEATURES_RF], com_alvo["y"])
    do_alvo = backtest[backtest["alvo"] == alvo]
    inferior, superior = faixa_erro(do_alvo["valor_real"], do_alvo["previsao_oficial"], QUANTIS_FAIXA)
    ridge_inferior, ridge_superior = faixa_erro(do_alvo["valor_real"], do_alvo["previsao_ridge"], QUANTIS_FAIXA)

    intercepto, coeficientes = coeficientes_originais(ridge_final, FEATURES_LINEAR)
    sazonal_origem = "ipca_sazonal_h1..h12 (composta)" if alvo == "acum_12m" else f"ipca_sazonal_h{meses_a_frente(alvo)}"
    for feature, coeficiente in [("intercepto", intercepto), *coeficientes.items()]:
        linhas_coeficientes.append({
            "alvo": alvo,
            "horizonte": meses_a_frente(alvo),
            "feature": feature,
            "coluna_origem": sazonal_origem if feature == "ipca_sazonal" else (None if feature == "intercepto" else feature),
            "coeficiente": coeficiente,
            "alpha": float(ridge_final.steps[-1][1].alpha_),
            "n_treino": len(com_alvo),
            "erro_p10": ridge_inferior,
            "erro_p90": ridge_superior,
        })

    atual = dados[dados["mes_base"] == mes_base_atual]
    temporais_atuais = {f"previsao_{m}": previsao_arima_alvo(p, mes_base_atual, alvo) for m, p in temporais_final.items()}
    previsao_oficial = float(mercado_atual.loc[alvo, MODELO_OFICIAL])
    linhas_previsao.append({
        "tipo": "previsao",
        "mes_base": mes_base_atual,
        "alvo": alvo,
        "mes_previsto": mes_base_atual + pd.DateOffset(months=meses_a_frente(alvo)),
        "periodo": None,
        "valor_real": None,
        **{f"previsao_{m}": float(np.asarray(previsao_referencia(atual, alvo, m))[0])
           for m in MODELOS_BASE if m not in (*SERIES_TEMPORAIS, "ridge", "random_forest")},
        **temporais_atuais,
        "previsao_ridge": float(ridge_final.predict(atual[FEATURES_LINEAR])[0]),
        "previsao_random_forest": float(rf_final.predict(atual[FEATURES_RF])[0]),
        **{f"previsao_{m}": float(mercado_atual.loc[alvo, m]) for m in ("focus", "top5", "media_tres", "media_tres_corrigida")},
        "previsao_oficial": previsao_oficial,
        "limite_inferior": previsao_oficial + inferior,
        "limite_superior": previsao_oficial + superior,
    })
    importancias += [{"alvo": alvo, "feature": f, "importancia": float(v)}
                     for f, v in zip(FEATURES_RF, rf_final.feature_importances_)]

mlflow.log_text(pd.DataFrame(linhas_coeficientes).to_csv(index=False), "coeficientes_ridge.csv")
mlflow.log_text(pd.DataFrame(importancias).to_csv(index=False), "importancia_random_forest.csv")
mlflow.log_text(tabela_parametros.to_csv(index=False), "parametros_arima.csv")
mlflow.log_text(tabela_correcao.to_csv(index=False), "correcao_modelo.csv")
run_id = run.info.run_id
mlflow.end_run()
print(f"MLflow run: {run_id}")

previsao_atual = pd.DataFrame(linhas_previsao)
display(previsao_atual[["alvo", "mes_previsto", "previsao_oficial", "limite_inferior", "limite_superior",
                        "previsao_arima", "previsao_focus", "previsao_top5"]].round(3))

# COMMAND ----------

# DBTITLE 1,Save Tables
def gravar(pdf, nome, comentario):
    tabela = f"{DESTINO}.{PREFIXO}{nome}"
    pdf = pdf.assign(run_id=run_id, treinado_em=agora)
    sdf = spark.createDataFrame(pdf)
    # O pandas representa ausência como NaN; na tabela, valor ausente deve ser NULL
    for coluna, tipo in sdf.dtypes:
        if tipo == "double":
            sdf = sdf.withColumn(coluna, when(isnan(coluna), None).otherwise(col(coluna)))
    (
        sdf.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabela)
    )
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{comentario}'")
    print(f"{tabela}: {len(pdf)} linhas")


previsoes = pd.concat([previsao_atual, backtest.assign(limite_inferior=None, limite_superior=None)], ignore_index=True)
previsoes["horizonte"] = previsoes["alvo"].map(meses_a_frente)
previsoes["mes_base"] = previsoes["mes_base"].dt.date
previsoes["mes_previsto"] = pd.to_datetime(previsoes["mes_previsto"]).dt.date
colunas_valores = ["valor_real", "previsao_oficial", "limite_inferior", "limite_superior",
                   *[f"previsao_{m}" for m in MODELOS]]
previsoes = previsoes[["tipo", "mes_base", "alvo", "horizonte", "mes_previsto", "periodo", *colunas_valores]]
previsoes = previsoes.astype({c: "float64" for c in colunas_valores})
previsoes["periodo"] = previsoes["periodo"].astype(object).where(previsoes["periodo"].notna(), None)

gravar(
    pd.DataFrame(linhas_coeficientes),
    "coeficientes_modelo",
    "Coeficientes do Ridge (modelo de comparação) por alvo, em unidades originais: "
    "previsao = intercepto + soma(coeficiente * feature). erro_p10/erro_p90 formam a faixa de 80% do Ridge",
)
gravar(
    tabela_parametros[["modelo", "parametro", "coeficiente", "erro_padrao", "p_valor", "ic95_inferior", "ic95_superior",
                       "n_meses"]],
    "parametros_arima",
    "Parâmetros do ARIMA, um dos componentes do modelo oficial, e dos SARIMAX, ajustados com todo o histórico: termos ARIMA e "
    "variáveis externas (mês anterior), com erro padrão, p-valor e intervalo de 95%",
)
gravar(
    tabela_correcao,
    "correcao_modelo",
    "Correção do modelo oficial: ajuste = intercepto + coeficiente_revisao * revisão do Top 5 em 4 semanas, somado à "
    "média de ARIMA, Focus e Top 5 nos alvos listados",
)
gravar(
    tabela_metricas,
    "metricas_modelo",
    "Erro por alvo, modelo e período na validação com origem móvel anual (anos de teste de 2005 em diante), em p.p.",
)
gravar(
    previsoes,
    "previsoes_ipca",
    "Previsões do IPCA (%): previsao_oficial = média de ARIMA, Focus e Top 5, com correção em h1; "
    "tipo previsao = a partir do último mês base, com faixa de 80%; "
    "tipo teste = previsto x realizado de cada modelo na validação com origem móvel anual",
)
