# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Treino do modelo de previsão do IPCA
# MAGIC
# MAGIC Treina um modelo por horizonte (estratégia direta) para prever o IPCA de cada um dos próximos 12 meses (`h1` a `h12`) e o IPCA acumulado em 12 meses (`acum_12m`), a partir da tabela `projeto_hands_on.gold.features_mensal`.
# MAGIC
# MAGIC | Modelo | Papel |
# MAGIC |---|---|
# MAGIC | `ultimo_mes` | Referência ingênua: repete o IPCA do mês base (ou o acumulado em 12 meses) |
# MAGIC | `sazonal` | Referência: média histórica do mês do calendário previsto |
# MAGIC | `ridge` | Regressão linear com regularização. **Modelo oficial**: os coeficientes vão para o simulador (`gold.coeficientes_modelo`) |
# MAGIC | `random_forest` | Comparação: modelo não linear com todas as features, sem uso no simulador |
# MAGIC
# MAGIC **Validação:** treino com meses base de 01/2006 a 12/2019, sem alvos depois de 12/2019 (para não vazar o período de teste), e teste nos meses base de 2020 em diante. As métricas (MAE e RMSE em pontos percentuais) e os parâmetros ficam registrados no MLflow.
# MAGIC
# MAGIC **Previsão atual:** depois da avaliação, o Ridge e o Random Forest são reajustados com todos os meses que têm alvo e aplicados ao mês base mais recente. A faixa de incerteza (80%) vem dos erros do Ridge no período de teste, por isso fica mais larga nos horizontes mais distantes.
# MAGIC
# MAGIC Tabelas gravadas: `gold.coeficientes_modelo`, `gold.metricas_modelo` e `gold.previsoes_ipca`.

# COMMAND ----------

# DBTITLE 1,Parameters
# Destino das tabelas; o teste em sandbox usa outro schema e um prefixo para não tocar na gold
dbutils.widgets.text("destino", "projeto_hands_on.gold", "Catálogo.schema de destino")
dbutils.widgets.text("prefixo_tabela", "", "Prefixo das tabelas de saída")

# COMMAND ----------

# DBTITLE 1,Environment
import os
from datetime import datetime, timezone

import mlflow
import numpy as np
import pandas as pd
from pyspark.sql.functions import col, isnan, when
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge, RidgeCV
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

ORIGEM = "projeto_hands_on.gold.features_mensal"
DESTINO = dbutils.widgets.get("destino").strip()
PREFIXO = dbutils.widgets.get("prefixo_tabela").strip()

FIM_TREINO = "2019-12-01"
HORIZONTES = list(range(1, 13))
# Nome do alvo -> coluna da features_mensal
ALVOS = {**{f"h{h}": f"alvo_ipca_h{h}" for h in HORIZONTES}, "acum_12m": "alvo_ipca_acum_12m"}

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
PARAMS_RF = {"n_estimators": 500, "min_samples_leaf": 3, "random_state": 42, "n_jobs": -1}
# Faixa de incerteza de 80%: quantis 10% e 90% dos erros do Ridge no teste
QUANTIS_FAIXA = (0.10, 0.90)

# COMMAND ----------

# DBTITLE 1,Functions
def meses_a_frente(alvo):
    """Months between the base month and the (last) forecast month: h3 -> 3, acum_12m -> 12."""
    return 12 if alvo == "acum_12m" else int(alvo[1:])


def preparar(df, alvo):
    """Adds the horizon-specific seasonal column 'ipca_sazonal' and the target column 'y'."""
    dados = df.copy()
    if alvo == "acum_12m":
        fatores = np.prod([1 + dados[f"ipca_sazonal_h{h}"] / 100 for h in HORIZONTES], axis=0)
        dados["ipca_sazonal"] = (fatores - 1) * 100
    else:
        dados["ipca_sazonal"] = dados[f"ipca_sazonal_h{meses_a_frente(alvo)}"]
    dados["y"] = dados[ALVOS[alvo]]
    return dados


def separar_treino_teste(dados, alvo, fim_treino):
    """Train: base months whose (last) target month is up to fim_treino. Test: base months after fim_treino.

    Base months up to fim_treino whose target falls after it are left out of both, so that no
    training target overlaps the test period.
    """
    com_alvo = dados[dados["y"].notna()]
    mes_alvo = com_alvo["mes_base"] + pd.DateOffset(months=meses_a_frente(alvo))
    limite = pd.Timestamp(fim_treino)
    return com_alvo[mes_alvo <= limite], com_alvo[com_alvo["mes_base"] > limite]


def previsao_referencia(dados, alvo, modelo):
    """Baseline forecasts: 'ultimo_mes' repeats the last known IPCA, 'sazonal' uses the historical mean."""
    if modelo == "sazonal":
        return dados["ipca_sazonal"]
    return dados["ipca_acum_12m"] if alvo == "acum_12m" else dados["ipca_lag1"]


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

# DBTITLE 1,Load Features
features = spark.read.table(ORIGEM).toPandas()
features["mes_base"] = pd.to_datetime(features["mes_base"])
features = features.sort_values("mes_base").reset_index(drop=True)

linha_atual = features[features["conjunto"] == "previsao"]
assert len(linha_atual) == 1, f"Esperada uma linha de previsão em {ORIGEM}, encontradas {len(linha_atual)}"
assert features["features_completas"].all(), "Há meses base com features nulas na tabela de origem"
mes_base_atual = linha_atual["mes_base"].iloc[0]
print(f"{len(features)} meses base, de {features['mes_base'].min():%m/%Y} a {mes_base_atual:%m/%Y}")
print(f"Previsão a partir do mês base {mes_base_atual:%m/%Y}")

# COMMAND ----------

# DBTITLE 1,Train, Evaluate and Forecast
usuario = spark.sql("SELECT current_user()").first()[0]
# No serverless, o MLflow tenta ler o registry da configuração do Spark (bloqueada) se ele não estiver definido
os.environ.setdefault("MLFLOW_REGISTRY_URI", "databricks-uc")
mlflow.set_experiment(f"/Users/{usuario}/previsao_ipca")
agora = datetime.now(timezone.utc).replace(tzinfo=None)

linhas_metricas, linhas_coeficientes, linhas_previsoes, importancias = [], [], [], []

with mlflow.start_run(run_name=f"treino_{agora:%Y%m%d_%H%M}") as run:
    mlflow.log_params({
        "fim_treino": FIM_TREINO,
        "mes_base_previsao": f"{mes_base_atual:%Y-%m}",
        "features_linear": ",".join(FEATURES_LINEAR),
        "features_rf": ",".join(FEATURES_RF),
        "alphas_ridge": f"{ALPHAS_RIDGE[0]:g} a {ALPHAS_RIDGE[-1]:g} ({len(ALPHAS_RIDGE)} valores)",
        **{f"rf_{k}": v for k, v in PARAMS_RF.items()},
    })

    for alvo in ALVOS:
        dados = preparar(features, alvo)
        treino, teste = separar_treino_teste(dados, alvo, FIM_TREINO)

        ridge = make_pipeline(StandardScaler(), RidgeCV(alphas=ALPHAS_RIDGE, cv=TimeSeriesSplit(n_splits=5)))
        ridge.fit(treino[FEATURES_LINEAR], treino["y"])
        alpha = float(ridge.steps[-1][1].alpha_)
        rf = RandomForestRegressor(**PARAMS_RF).fit(treino[FEATURES_RF], treino["y"])

        previstos = {
            "ultimo_mes": previsao_referencia(teste, alvo, "ultimo_mes"),
            "sazonal": previsao_referencia(teste, alvo, "sazonal"),
            "ridge": ridge.predict(teste[FEATURES_LINEAR]),
            "random_forest": rf.predict(teste[FEATURES_RF]),
        }
        inferior, superior = faixa_erro(teste["y"], previstos["ridge"], QUANTIS_FAIXA)

        for modelo, valores in previstos.items():
            m = metricas(teste["y"], valores)
            linhas_metricas.append({"alvo": alvo, "horizonte": meses_a_frente(alvo), "modelo": modelo, **m,
                                    "n_treino": len(treino)})
            mlflow.log_metrics({f"mae_{modelo}_{alvo}": m["mae"], f"rmse_{modelo}_{alvo}": m["rmse"]})

        # Previsões do período de teste (backtest), para comparar previsto x realizado
        for i, (_, linha) in enumerate(teste.iterrows()):
            linhas_previsoes.append({
                "tipo": "teste",
                "mes_base": linha["mes_base"],
                "alvo": alvo,
                "valor_real": float(linha["y"]),
                **{f"previsao_{modelo}": float(np.asarray(valores)[i]) for modelo, valores in previstos.items()},
                "limite_inferior": None,
                "limite_superior": None,
            })

        # Modelos finais: mesmo alpha e mesmos parâmetros, reajustados com todos os meses que têm alvo
        com_alvo = dados[dados["y"].notna()]
        ridge_final = make_pipeline(StandardScaler(), Ridge(alpha=alpha)).fit(com_alvo[FEATURES_LINEAR], com_alvo["y"])
        rf_final = RandomForestRegressor(**PARAMS_RF).fit(com_alvo[FEATURES_RF], com_alvo["y"])

        intercepto, coeficientes = coeficientes_originais(ridge_final, FEATURES_LINEAR)
        coluna_sazonal = "ipca_sazonal_h1..h12 (composta)" if alvo == "acum_12m" else f"ipca_sazonal_h{meses_a_frente(alvo)}"
        for feature, coeficiente in [("intercepto", intercepto), *coeficientes.items()]:
            linhas_coeficientes.append({
                "alvo": alvo,
                "horizonte": meses_a_frente(alvo),
                "feature": feature,
                "coluna_origem": coluna_sazonal if feature == "ipca_sazonal" else (None if feature == "intercepto" else feature),
                "coeficiente": coeficiente,
                "alpha": alpha,
                "n_treino": len(com_alvo),
                "erro_p10": inferior,
                "erro_p90": superior,
            })

        atual = dados[dados["mes_base"] == mes_base_atual]
        previsao_ridge = float(ridge_final.predict(atual[FEATURES_LINEAR])[0])
        linhas_previsoes.append({
            "tipo": "previsao",
            "mes_base": mes_base_atual,
            "alvo": alvo,
            "valor_real": None,
            "previsao_ultimo_mes": float(previsao_referencia(atual, alvo, "ultimo_mes").iloc[0]),
            "previsao_sazonal": float(previsao_referencia(atual, alvo, "sazonal").iloc[0]),
            "previsao_ridge": previsao_ridge,
            "previsao_random_forest": float(rf_final.predict(atual[FEATURES_RF])[0]),
            "limite_inferior": previsao_ridge + inferior,
            "limite_superior": previsao_ridge + superior,
        })
        importancias += [{"alvo": alvo, "feature": f, "importancia": float(v)}
                         for f, v in zip(FEATURES_RF, rf.feature_importances_)]

    tabela_metricas = pd.DataFrame(linhas_metricas)
    mensais = tabela_metricas[tabela_metricas["alvo"] != "acum_12m"].groupby("modelo")["mae"].mean()
    mlflow.log_metrics({f"mae_medio_h1_h12_{modelo}": float(v) for modelo, v in mensais.items()})
    mlflow.log_text(tabela_metricas.to_csv(index=False), "metricas.csv")
    mlflow.log_text(pd.DataFrame(linhas_coeficientes).to_csv(index=False), "coeficientes_ridge.csv")
    mlflow.log_text(pd.DataFrame(importancias).to_csv(index=False), "importancia_random_forest.csv")
    run_id = run.info.run_id

print(f"MLflow run: {run_id}")

# COMMAND ----------

# DBTITLE 1,Model Comparison
# MAE no teste (p.p.), por alvo e modelo: quanto menor, melhor
comparacao = tabela_metricas.pivot(index=["horizonte", "alvo"], columns="modelo", values="mae").round(3)
comparacao["melhor"] = comparacao.idxmin(axis=1)
display(comparacao.reset_index())
print("MAE médio de h1 a h12:")
print(mensais.round(3).sort_values().to_string())

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


previsoes = pd.DataFrame(linhas_previsoes)
previsoes["horizonte"] = previsoes["alvo"].map(meses_a_frente)
previsoes["mes_previsto"] = [
    (base + pd.DateOffset(months=h)).date() for base, h in zip(previsoes["mes_base"], previsoes["horizonte"])
]
previsoes["mes_base"] = previsoes["mes_base"].dt.date
colunas_previsoes = [
    "tipo", "mes_base", "alvo", "horizonte", "mes_previsto", "valor_real", "previsao_ridge", "limite_inferior",
    "limite_superior", "previsao_random_forest", "previsao_ultimo_mes", "previsao_sazonal",
]
previsoes = previsoes[colunas_previsoes].astype({c: "float64" for c in colunas_previsoes[5:]})

gravar(
    pd.DataFrame(linhas_coeficientes),
    "coeficientes_modelo",
    "Coeficientes do Ridge por alvo (h1 a h12 e acum_12m), em unidades originais: "
    "previsao = intercepto + soma(coeficiente * feature). erro_p10/erro_p90 formam a faixa de 80%",
)
gravar(tabela_metricas, "metricas_modelo", "Erro no teste (meses base de 2020 em diante) por alvo e modelo, em p.p.")
gravar(
    previsoes,
    "previsoes_ipca",
    "Previsões do IPCA (%): tipo previsao = a partir do último mês base, com faixa de 80%; "
    "tipo teste = previsto x realizado no período de teste",
)
