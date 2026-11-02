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
# MAGIC | `ridge` | Regressão | Regressão linear com regularização; os coeficientes vão para `gold.coeficientes_modelo`, base do simulador |
# MAGIC | `random_forest` | Regressão | Comparação não linear com todas as features |
# MAGIC
# MAGIC **Validação (origem móvel anual):** para cada ano de teste, de 2005 até o ano atual, os modelos são treinados só com meses cujo alvo é anterior a esse ano e testados nos meses base do ano. O erro é resumido por período: 2005–2016, 2017–2019 (inflação baixa e estável), 2020–2021 (pandemia) e 2022 em diante. Métricas e parâmetros ficam registrados no MLflow.
# MAGIC
# MAGIC **Previsão atual:** os modelos são reajustados com todo o histórico e aplicados ao mês base mais recente. A faixa de incerteza (80%) vem dos erros do Ridge em todos os anos de teste.
# MAGIC
# MAGIC Tabelas gravadas: `gold.coeficientes_modelo`, `gold.metricas_modelo` e `gold.previsoes_ipca`.

# COMMAND ----------

# DBTITLE 1,Parameters
# Origem e destino; o teste em sandbox aponta para outras tabelas e usa um prefixo para não tocar na gold
dbutils.widgets.text("origem", "projeto_hands_on.gold.features_mensal", "Tabela de features")
dbutils.widgets.text("destino", "projeto_hands_on.gold", "Catálogo.schema de destino")
dbutils.widgets.text("prefixo_tabela", "", "Prefixo das tabelas de saída")

# COMMAND ----------

# DBTITLE 1,Environment
import os
import warnings
from datetime import datetime, timezone

import mlflow
import numpy as np
import pandas as pd
from pyspark.sql.functions import col, isnan, when
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import RidgeCV
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.statespace.sarimax import SARIMAX

ORIGEM = dbutils.widgets.get("origem").strip()
DESTINO = dbutils.widgets.get("destino").strip()
PREFIXO = dbutils.widgets.get("prefixo_tabela").strip()
IPCA_SILVER = "projeto_hands_on.silver.ipca"

# Primeiro ano de teste: a tabela de features começa em 2001, então o primeiro treino tem 4 anos
PRIMEIRO_ANO_TESTE = 2005
# Histórico do IPCA para o ARIMA e as médias móveis: depois da estabilização do Plano Real
INICIO_HISTORICO_IPCA = "1997-01-01"
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
MODELOS = ["ultimo_mes", "media_movel_3m", "media_movel_6m", "media_movel_12m", "sazonal", "arima", "ridge",
           "random_forest"]

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
# Faixa de incerteza de 80%: quantis 10% e 90% dos erros do Ridge nos testes
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


def ajustar_arima(serie, ordem, sazonal):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return SARIMAX(serie, order=ordem, seasonal_order=sazonal, trend="c").fit(disp=False)


def escolher_ordem_arima(serie):
    """Picks the (order, seasonal order) with the lowest AIC on the given history."""
    candidatos = [(ajustar_arima(serie, o, s).aic, o, s) for o in ORDENS_ARIMA for s in SAZONAIS_ARIMA]
    _, ordem, sazonal = min(candidatos, key=lambda c: c[0])
    return ordem, sazonal


def previsoes_arima(serie, ordem, sazonal, fim_treino, origens):
    """Fits the ARIMA on the series up to fim_treino and, keeping those parameters, forecasts the next 12
    months from each origin using only the data up to that origin. Returns {origin: array of 12 forecasts}."""
    ajuste = ajustar_arima(serie[serie.index <= fim_treino], ordem, sazonal)
    previsoes = {}
    for origem in origens:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            filtrado = ajuste.apply(serie[serie.index <= origem])
        previsoes[origem] = np.asarray(filtrado.forecast(12), dtype=float)
    return previsoes


def previsao_arima_alvo(previsoes, origem, alvo):
    valores = previsoes[origem]
    return float(compor(valores)) if alvo == "acum_12m" else float(valores[meses_a_frente(alvo) - 1])


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

linha_atual = features[features["conjunto"] == "previsao"]
assert len(linha_atual) == 1, f"Esperada uma linha de previsão em {ORIGEM}, encontradas {len(linha_atual)}"
assert features["features_completas"].all(), "Há meses base com features nulas na tabela de origem"
mes_base_atual = linha_atual["mes_base"].iloc[0]
assert serie_ipca.index[-1] == mes_base_atual, "O último IPCA da silver não é o mês base mais recente das features"
anos_teste = list(range(PRIMEIRO_ANO_TESTE, mes_base_atual.year + 1))
print(f"{len(features)} meses base, de {features['mes_base'].min():%m/%Y} a {mes_base_atual:%m/%Y}")
print(f"Anos de teste: {anos_teste[0]} a {anos_teste[-1]}; previsão a partir de {mes_base_atual:%m/%Y}")

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

for ano in anos_teste:
    fim_treino = pd.Timestamp(year=ano - 1, month=12, day=1)
    origens = list(features.loc[features["mes_base"].dt.year == ano, "mes_base"])
    arima = previsoes_arima(serie_ipca, ordem_arima, sazonal_arima, fim_treino, origens)

    for alvo, dados in preparados.items():
        treino, teste = separar_por_ano(dados, alvo, ano)
        if teste.empty:
            continue
        ridge = novo_ridge().fit(treino[FEATURES_LINEAR], treino["y"])
        rf = RandomForestRegressor(**PARAMS_RF).fit(treino[FEATURES_RF], treino["y"])
        previstos = {modelo: np.asarray(previsao_referencia(teste, alvo, modelo), dtype=float)
                     for modelo in MODELOS if modelo not in ("arima", "ridge", "random_forest")}
        previstos["arima"] = np.array([previsao_arima_alvo(arima, o, alvo) for o in teste["mes_base"]])
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
                **{f"previsao_{modelo}": float(previstos[modelo][i]) for modelo in MODELOS},
            })
    print(f"{ano}: {len(origens)} meses base testados")

backtest = pd.DataFrame(linhas_teste)

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
    "mes_base_previsao": f"{mes_base_atual:%Y-%m}",
    "arima_order": str(ordem_arima),
    "arima_seasonal_order": str(sazonal_arima),
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
arima_final = previsoes_arima(serie_ipca, ordem_arima, sazonal_arima, mes_base_atual, [mes_base_atual])
linhas_coeficientes, linhas_previsao, importancias = [], [], []

for alvo, dados in preparados.items():
    com_alvo = dados[dados["y"].notna()]
    ridge_final = novo_ridge().fit(com_alvo[FEATURES_LINEAR], com_alvo["y"])
    rf_final = RandomForestRegressor(**PARAMS_RF).fit(com_alvo[FEATURES_RF], com_alvo["y"])
    do_alvo = backtest[backtest["alvo"] == alvo]
    inferior, superior = faixa_erro(do_alvo["valor_real"], do_alvo["previsao_ridge"], QUANTIS_FAIXA)

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
            "erro_p10": inferior,
            "erro_p90": superior,
        })

    atual = dados[dados["mes_base"] == mes_base_atual]
    previsao_ridge = float(ridge_final.predict(atual[FEATURES_LINEAR])[0])
    linhas_previsao.append({
        "tipo": "previsao",
        "mes_base": mes_base_atual,
        "alvo": alvo,
        "mes_previsto": mes_base_atual + pd.DateOffset(months=meses_a_frente(alvo)),
        "periodo": None,
        "valor_real": None,
        **{f"previsao_{m}": float(np.asarray(previsao_referencia(atual, alvo, m))[0])
           for m in MODELOS if m not in ("arima", "ridge", "random_forest")},
        "previsao_arima": previsao_arima_alvo(arima_final, mes_base_atual, alvo),
        "previsao_ridge": previsao_ridge,
        "previsao_random_forest": float(rf_final.predict(atual[FEATURES_RF])[0]),
        "limite_inferior": previsao_ridge + inferior,
        "limite_superior": previsao_ridge + superior,
    })
    importancias += [{"alvo": alvo, "feature": f, "importancia": float(v)}
                     for f, v in zip(FEATURES_RF, rf_final.feature_importances_)]

mlflow.log_text(pd.DataFrame(linhas_coeficientes).to_csv(index=False), "coeficientes_ridge.csv")
mlflow.log_text(pd.DataFrame(importancias).to_csv(index=False), "importancia_random_forest.csv")
run_id = run.info.run_id
mlflow.end_run()
print(f"MLflow run: {run_id}")

previsao_atual = pd.DataFrame(linhas_previsao)
display(previsao_atual[["alvo", "mes_previsto", "previsao_ridge", "limite_inferior", "limite_superior",
                        "previsao_arima", "previsao_sazonal", "previsao_random_forest"]].round(3))

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
colunas_valores = ["valor_real", "previsao_ridge", "limite_inferior", "limite_superior",
                   *[f"previsao_{m}" for m in MODELOS if m != "ridge"]]
previsoes = previsoes[["tipo", "mes_base", "alvo", "horizonte", "mes_previsto", "periodo", *colunas_valores]]
previsoes = previsoes.astype({c: "float64" for c in colunas_valores})
previsoes["periodo"] = previsoes["periodo"].astype(object).where(previsoes["periodo"].notna(), None)

gravar(
    pd.DataFrame(linhas_coeficientes),
    "coeficientes_modelo",
    "Coeficientes do Ridge por alvo (h1 a h12 e acum_12m), em unidades originais: "
    "previsao = intercepto + soma(coeficiente * feature). erro_p10/erro_p90 formam a faixa de 80%",
)
gravar(
    tabela_metricas,
    "metricas_modelo",
    "Erro por alvo, modelo e período na validação com origem móvel anual (anos de teste de 2005 em diante), em p.p.",
)
gravar(
    previsoes,
    "previsoes_ipca",
    "Previsões do IPCA (%): tipo previsao = a partir do último mês base, com faixa de 80%; "
    "tipo teste = previsto x realizado de cada modelo na validação com origem móvel anual",
)
