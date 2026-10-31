from pyspark import pipelines as dp
from pyspark.sql.functions import avg, col, count, date_trunc, exp, log1p, max_by, pow, sum as spark_sum, to_date

# Dias úteis por ano usados pelo mercado brasileiro para anualizar taxas diárias
DIAS_UTEIS_ANO = 252


@dp.materialized_view(
    name="projeto_hands_on.silver.selic",
    comment="Taxa Selic meta (% a.a.), mensal: valor vigente no último dia do mês, datado no primeiro dia",
)
@dp.expect_or_fail("data_ref_valida", "data_ref IS NOT NULL")
# Desde 1999 a meta ficou entre 2% e 45% a.a.; fora disso, apenas registra para investigação
@dp.expect("faixa_plausivel", "valor > 0 AND valor <= 100")
def selic():
    # Último valor do mês em vez do MAX: o MAX pegava a taxa anterior ao corte nos meses de queda
    return (
        spark.read.table("bronze_selic")
        .withColumn("valor", col("valor").cast("DECIMAL(18,6)"))
        .withColumn("data_ref", to_date(col("data"), "dd/MM/yyyy"))
        .dropDuplicates(["data_ref"])
        .withColumn("month_start", date_trunc("month", col("data_ref")).cast("date"))
        .groupBy("month_start")
        .agg(max_by("valor", "data_ref").alias("valor"))
        .select(col("month_start").alias("data_ref"), "valor")
    )


@dp.materialized_view(
    name="projeto_hands_on.silver.cdi",
    comment=(
        "Taxa CDI mensal, datada no primeiro dia: valor = média da taxa anualizada (% a.a., 252 dias úteis), "
        "acumulado_mes = taxa acumulada no mês (% no mês), dias_uteis = dias com taxa no mês"
    ),
)
@dp.expect_or_fail("data_ref_valida", "data_ref IS NOT NULL")
# Valores muito altos são válidos no período de hiperinflação (até ~1,8 milhão % a.a. anualizado), então só exige não negativos
@dp.expect_all({
    "taxas_nao_negativas": "valor >= 0 AND acumulado_mes >= 0",
    "dias_uteis_no_mes": "dias_uteis BETWEEN 1 AND 23",
})
def cdi():
    # A série 12 é uma taxa diária (% a.d.): anualiza para comparar com a Selic e
    # compõe os dias do mês para comparar com o IPCA (% a.m.)
    taxa_dia = col("valor_dia") / 100
    return (
        spark.read.table("bronze_cdi")
        .withColumn("valor_dia", col("valor").cast("DOUBLE"))
        .withColumn("data_ref", to_date(col("data"), "dd/MM/yyyy"))
        .dropDuplicates(["data_ref"])
        .withColumn("month_start", date_trunc("month", col("data_ref")).cast("date"))
        .withColumn("taxa_anual", (pow(1 + taxa_dia, DIAS_UTEIS_ANO) - 1) * 100)
        .groupBy("month_start")
        .agg(
            avg("taxa_anual").alias("valor"),
            ((exp(spark_sum(log1p(taxa_dia))) - 1) * 100).alias("acumulado_mes"),
            count("*").alias("dias_uteis"),
        )
        .select(
            col("month_start").alias("data_ref"),
            col("valor").cast("DECIMAL(18,6)").alias("valor"),
            col("acumulado_mes").cast("DECIMAL(18,6)").alias("acumulado_mes"),
            "dias_uteis",
        )
    )


@dp.materialized_view(
    name="projeto_hands_on.silver.tr",
    comment="Taxa TR diária (inclui data de fim do período)",
)
@dp.expect_or_fail("data_ref_valida", "data_ref IS NOT NULL")
# TR igual a zero é válida (calculada como zero quando a Selic está baixa)
@dp.expect_all({
    "taxa_nao_negativa": "valor >= 0",
    "periodo_valido": "data_fim > data_ref",
})
def tr():
    return (
        spark.read.table("bronze_tr")
        .withColumn("data_ref", to_date(col("data"), "dd/MM/yyyy"))
        .withColumn("data_fim", to_date(col("dataFim"), "dd/MM/yyyy"))
        .withColumn("valor", col("valor").cast("DECIMAL(18,6)"))
        .select("data_ref", "data_fim", "valor")
    )


@dp.materialized_view(
    name="projeto_hands_on.silver.ipca",
    comment="Variação percentual mensal do IPCA",
)
@dp.expect_or_fail("data_ref_valida", "data_ref IS NOT NULL")
# Inclui o período de hiperinflação (máximo de 82,39% em 03/1990) e deflações pontuais
@dp.expect("faixa_plausivel", "valor BETWEEN -5 AND 100")
def ipca():
    # A carga histórica em janelas de 10 anos gravou em duplicidade o mês de início de cada janela
    # (01/1990, 01/2000, 01/2010, 01/2020): em séries mensais a API devolve também o mês anterior a dataInicial
    return (
        spark.read.table("bronze_ipca")
        .withColumn("data_ref", to_date(col("data"), "dd/MM/yyyy"))
        .withColumn("valor", col("valor").cast("DECIMAL(18,6)"))
        .dropDuplicates(["data_ref"])
        .select("data_ref", "valor")
    )
