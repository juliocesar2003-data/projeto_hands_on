from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql.functions import (
    add_months,
    avg,
    col,
    count,
    current_date,
    date_trunc,
    exp,
    lag,
    lead,
    log,
    log1p,
    max as spark_max,
    max_by,
    month,
    months_between,
    round as spark_round,
    stddev,
    sum as spark_sum,
    when,
    year,
)

# Metas de inflação do CMN (IPCA, % no ano-calendário até 2024; meta contínua em 12 meses desde 2025).
# Fonte: https://www.bcb.gov.br/controleinflacao/historicometas (conferido em 31/10/2026).
# Ano -> (meta %, tamanho do intervalo de tolerância +/- p.p., norma)
METAS_INFLACAO = {
    2005: (4.50, 2.50, "Resolução CMN nº 3.108"),
    2006: (4.50, 2.00, "Resolução CMN nº 3.210"),
    2007: (4.50, 2.00, "Resolução CMN nº 3.291"),
    2008: (4.50, 2.00, "Resolução CMN nº 3.378"),
    2009: (4.50, 2.00, "Resolução CMN nº 3.463"),
    2010: (4.50, 2.00, "Resolução CMN nº 3.584"),
    2011: (4.50, 2.00, "Resolução CMN nº 3.748"),
    2012: (4.50, 2.00, "Resolução CMN nº 3.880"),
    2013: (4.50, 2.00, "Resolução CMN nº 3.991"),
    2014: (4.50, 2.00, "Resolução CMN nº 4.095"),
    2015: (4.50, 2.00, "Resolução CMN nº 4.237"),
    2016: (4.50, 2.00, "Resolução CMN nº 4.345"),
    2017: (4.50, 1.50, "Resolução CMN nº 4.419"),
    2018: (4.50, 1.50, "Resolução CMN nº 4.499"),
    2019: (4.25, 1.50, "Resolução CMN nº 4.582"),
    2020: (4.00, 1.50, "Resolução CMN nº 4.582"),
    2021: (3.75, 1.50, "Resolução CMN nº 4.671"),
    2022: (3.50, 1.50, "Resolução CMN nº 4.724"),
    2023: (3.25, 1.50, "Resolução CMN nº 4.831"),
    2024: (3.00, 1.50, "Resolução CMN nº 4.918"),
    2025: (3.00, 1.50, "Resolução CMN nº 5.141 (meta contínua)"),
    2026: (3.00, 1.50, "Resolução CMN nº 5.141 (meta contínua)"),
    2027: (3.00, 1.50, "Resolução CMN nº 5.141 (meta contínua)"),
}

# Primeiro mês base da tabela (20 anos de histórico); os lags usam os meses anteriores
INICIO_FEATURES = "2006-01-01"
# Período de treino do modelo: média e desvio padrão dos z-scores vêm só dele, para não vazar o futuro
INICIO_TREINO = "2006-01-01"
FIM_TREINO = "2019-12-01"
# Médias sazonais a partir do regime de metas (1999), sem o período de inflação alta dos anos 1990
INICIO_SAZONAL = "2000-01-01"
HORIZONTES = range(1, 13)

# Variações comparadas em z-score (desvios padrão em relação à média do treino)
COLUNAS_Z = ["usd_var_12m", "selic_var_12m_pp", "ipca_acum_12m"]
COMPARACOES_Z = {
    "usd_vs_selic_z": ("usd_var_12m_z", "selic_var_12m_pp_z"),
    "ipca_vs_selic_z": ("ipca_acum_12m_z", "selic_var_12m_pp_z"),
    "usd_vs_ipca_z": ("usd_var_12m_z", "ipca_acum_12m_z"),
}

FEATURES = (
    ["ipca_lag1", "ipca_lag2", "ipca_lag3", "ipca_lag12", "ipca_acum_3m", "ipca_acum_12m", "ipca_menos_sazonal_lag1"]
    + [f"ipca_sazonal_h{h}" for h in HORIZONTES]
    + ["meta_inflacao", "ipca_12m_menos_meta"]
    + ["selic_lag1", "selic_var_3m_pp", "selic_var_12m_pp", "meses_desde_mudanca_selic", "juro_real_12m"]
    + ["usd_lag1", "usd_var_lag1", "usd_var_3m", "usd_var_12m", "usd_volatilidade_lag1", "usd_menos_ipca_12m"]
    + [f"{c}_z" for c in COLUNAS_Z]
    + list(COMPARACOES_Z)
)
ALVOS = [f"alvo_ipca_h{h}" for h in HORIZONTES] + ["alvo_ipca_acum_12m"]


@dp.materialized_view(
    name="projeto_hands_on.gold.metas_inflacao",
    comment="Metas de inflação definidas pelo CMN por ano (IPCA, %), com intervalo de tolerância e norma",
)
@dp.expect_all_or_fail({
    "ano_valido": "ano IS NOT NULL",
    "meta_positiva": "meta > 0 AND tolerancia_pp > 0",
})
def metas_inflacao():
    linhas = [(ano, meta, tol, norma) for ano, (meta, tol, norma) in METAS_INFLACAO.items()]
    return spark.createDataFrame(linhas, "ano INT, meta DOUBLE, tolerancia_pp DOUBLE, norma STRING").select(
        "ano", "meta", "tolerancia_pp", (col("meta") - col("tolerancia_pp")).alias("limite_inferior"),
        (col("meta") + col("tolerancia_pp")).alias("limite_superior"), "norma",
    )


def _usd_mensal():
    """USD/BRL per month: closing rate on the last business day and volatility of the daily log returns (%)."""
    diario = Window.orderBy("data_cotacao")
    return (
        spark.read.table("projeto_hands_on.silver.moedas_conversao")
        .filter(col("moeda") == "USD")
        .select("data_cotacao", col("valor_brl").cast("double").alias("usd"))
        .withColumn("retorno", log(col("usd") / lag("usd").over(diario)))
        .groupBy(date_trunc("month", col("data_cotacao")).cast("date").alias("mes_base"))
        .agg(max_by("usd", "data_cotacao").alias("usd"), (stddev("retorno") * 100).alias("usd_volatilidade_lag1"))
    )


@dp.materialized_view(
    name="projeto_hands_on.gold.features_mensal",
    comment=(
        "Base do modelo de previsão do IPCA: uma linha por mês base (último mês com IPCA divulgado), "
        "com as features conhecidas até o fim do mês base e os alvos dos 12 meses seguintes (h1 a h12)"
    ),
)
@dp.expect_or_fail("mes_base_valido", "mes_base IS NOT NULL")
# Lacunas nas séries ou meta de inflação não cadastrada para o ano: apenas registra para investigação
@dp.expect_all({
    "features_completas": "features_completas",
    "meta_cadastrada": "meta_inflacao IS NOT NULL",
    "ipca_plausivel": "ipca_lag1 BETWEEN -3 AND 5",
    "selic_plausivel": "selic_lag1 > 0 AND selic_lag1 <= 50",
    "dolar_plausivel": "usd_lag1 > 0",
})
def features_mensal():
    w = Window.orderBy("mes_base")
    ultimos_3 = w.rowsBetween(-2, 0)
    ultimos_12 = w.rowsBetween(-11, 0)
    proximos_12 = w.rowsBetween(1, 12)
    # Média do IPCA do mesmo mês do calendário até cada mês (inclusive), desde INICIO_SAZONAL
    mesmo_mes = Window.partitionBy(month("mes_base")).orderBy("mes_base").rowsBetween(Window.unboundedPreceding, 0)

    def acumulado(janela, meses):
        # Variação composta em %; nula se a janela não tem todos os meses
        return when(
            count("ipca").over(janela) == meses,
            (exp(spark_sum(log1p(col("ipca") / 100)).over(janela)) - 1) * 100,
        )

    def variacao(coluna, meses):
        return (col(coluna) / lag(coluna, meses).over(w) - 1) * 100

    ipca = spark.read.table("projeto_hands_on.silver.ipca").select(
        col("data_ref").alias("mes_base"), col("valor").cast("double").alias("ipca")
    )
    selic = spark.read.table("projeto_hands_on.silver.selic").select(
        col("data_ref").alias("mes_base"), col("valor").cast("double").alias("selic")
    )
    metas = spark.read.table("projeto_hands_on.gold.metas_inflacao").select(
        col("ano"), col("meta").alias("meta_inflacao")
    )

    base = (
        ipca.join(selic, "mes_base", "left")
        .join(_usd_mensal(), "mes_base", "left")
        .join(metas, year("mes_base") == col("ano"), "left")
        .drop("ano")
        .withColumn("media_mesmo_mes", avg(when(col("mes_base") >= INICIO_SAZONAL, col("ipca"))).over(mesmo_mes))
        .withColumn("mudou_selic", col("selic") != lag("selic").over(w))
        .withColumn(
            "ultima_mudanca_selic",
            spark_max(when(col("mudou_selic"), col("mes_base"))).over(w.rowsBetween(Window.unboundedPreceding, 0)),
        )
    )

    # lagN = N meses antes do mês previsto em h1, então lag1 é o próprio mês base
    features = base.select(
        "mes_base",
        month("mes_base").alias("mes_calendario"),
        col("ipca").alias("ipca_lag1"),
        lag("ipca", 1).over(w).alias("ipca_lag2"),
        lag("ipca", 2).over(w).alias("ipca_lag3"),
        lag("ipca", 11).over(w).alias("ipca_lag12"),
        acumulado(ultimos_3, 3).alias("ipca_acum_3m"),
        acumulado(ultimos_12, 12).alias("ipca_acum_12m"),
        # Comparado com a média dos anos anteriores (sem o próprio mês)
        (col("ipca") - lag("media_mesmo_mes", 12).over(w)).alias("ipca_menos_sazonal_lag1"),
        # Média histórica do mês do calendário de cada horizonte, só com anos já conhecidos no mês base
        *[lag("media_mesmo_mes", 12 - h).over(w).alias(f"ipca_sazonal_h{h}") for h in HORIZONTES],
        "meta_inflacao",
        col("selic").alias("selic_lag1"),
        (col("selic") - lag("selic", 3).over(w)).alias("selic_var_3m_pp"),
        (col("selic") - lag("selic", 12).over(w)).alias("selic_var_12m_pp"),
        months_between("mes_base", "ultima_mudanca_selic").cast("int").alias("meses_desde_mudanca_selic"),
        col("usd").alias("usd_lag1"),
        variacao("usd", 1).alias("usd_var_lag1"),
        variacao("usd", 3).alias("usd_var_3m"),
        variacao("usd", 12).alias("usd_var_12m"),
        "usd_volatilidade_lag1",
        *[lead("ipca", h).over(w).alias(f"alvo_ipca_h{h}") for h in HORIZONTES],
        acumulado(proximos_12, 12).alias("alvo_ipca_acum_12m"),
    ).filter(col("mes_base") >= INICIO_FEATURES)

    features = (
        features.withColumn("ipca_12m_menos_meta", col("ipca_acum_12m") - col("meta_inflacao"))
        .withColumn("juro_real_12m", col("selic_lag1") - col("ipca_acum_12m"))
        .withColumn("usd_menos_ipca_12m", col("usd_var_12m") - col("ipca_acum_12m"))
    )

    # z-score = (valor - média do treino) / desvio padrão do treino
    estatisticas = (
        features.filter(col("mes_base").between(INICIO_TREINO, FIM_TREINO))
        .agg(*[f(c).alias(f"{c}_{nome}") for c in COLUNAS_Z for nome, f in (("media", avg), ("desvio", stddev))])
    )
    features = features.crossJoin(estatisticas)
    for c in COLUNAS_Z:
        features = features.withColumn(f"{c}_z", (col(c) - col(f"{c}_media")) / col(f"{c}_desvio"))
    for nome, (a, b) in COMPARACOES_Z.items():
        features = features.withColumn(nome, col(a) - col(b))

    alvos_disponiveis = sum(col(f"alvo_ipca_h{h}").isNotNull().cast("int") for h in HORIZONTES)
    features_completas = col(FEATURES[0]).isNotNull()
    for c in FEATURES[1:]:
        features_completas = features_completas & col(c).isNotNull()

    colunas = [col(c) if c == "meses_desde_mudanca_selic" else spark_round(c, 4).alias(c) for c in FEATURES + ALVOS]
    return features.select(
        "mes_base",
        add_months("mes_base", 1).alias("mes_h1"),
        "mes_calendario",
        *colunas,
        alvos_disponiveis.alias("alvos_disponiveis"),
        features_completas.alias("features_completas"),
        when(col("mes_base") <= FIM_TREINO, "treino")
        .when(alvos_disponiveis > 0, "teste")
        .otherwise("previsao")
        .alias("conjunto"),
        current_date().alias("data_calculo"),
    )
