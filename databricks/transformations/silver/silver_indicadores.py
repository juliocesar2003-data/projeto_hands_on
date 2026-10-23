from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date, date_trunc, max as spark_max


@dp.materialized_view(
    name="projeto_hands_on.silver.selic",
    comment="Taxa Selic meta, mensal (agrupamento por mês, MAX, primeiro dia)",
)
def selic():
    return (
        spark.read.table("bronze_selic")
        .withColumn("valor", col("valor").cast("DECIMAL(18,6)"))
        .withColumn("data_ref", to_date(col("data"), "dd/MM/yyyy"))
        .withColumn("month_start", date_trunc("month", col("data_ref")).cast("date"))
        .groupBy("month_start")
        .agg(spark_max("valor").alias("valor"))
        .select(col("month_start").alias("data_ref"), "valor")
    )


@dp.materialized_view(
    name="projeto_hands_on.silver.cdi",
    comment="Taxa CDI diária, mensal (agrupamento por mês, MAX, primeiro dia)",
)
def cdi():
    return (
        spark.read.table("bronze_cdi")
        .withColumn("valor", col("valor").cast("DECIMAL(18,6)"))
        .withColumn("data_ref", to_date(col("data"), "dd/MM/yyyy"))
        .withColumn("month_start", date_trunc("month", col("data_ref")).cast("date"))
        .groupBy("month_start")
        .agg(spark_max("valor").alias("valor"))
        .select(col("month_start").alias("data_ref"), "valor")
    )


@dp.materialized_view(
    name="projeto_hands_on.silver.tr",
    comment="Taxa TR diária (inclui data de fim do período)",
)
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
def ipca():
    return (
        spark.read.table("bronze_ipca")
        .withColumn("data_ref", to_date(col("data"), "dd/MM/yyyy"))
        .withColumn("valor", col("valor").cast("DECIMAL(18,6)"))
        .select("data_ref", "valor")
    )
