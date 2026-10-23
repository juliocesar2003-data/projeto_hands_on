from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date, when


@dp.materialized_view(
    name="projeto_hands_on.silver.moedas_conversao",
    comment="Cotações de câmbio consolidadas e tipadas (EUR, USD, GBP, JPY, ARS)",
)
def moedas_conversao():
    return (
        spark.read.table("bronze_moedas")
        .withColumn(
            "moeda",
            when(col("moeda_arquivo") == "euro", "EUR")
            .when(col("moeda_arquivo") == "dolar", "USD")
            .when(col("moeda_arquivo") == "libra", "GBP")
            .when(col("moeda_arquivo") == "iene", "JPY")
            .when(col("moeda_arquivo") == "peso_argentino", "ARS"),
        )
        .withColumn("data_cotacao", to_date(col("data"), "dd/MM/yyyy"))
        .withColumn("valor_brl", col("valor").cast("DECIMAL(18,6)"))
        .filter(col("data").isNotNull() & col("valor").isNotNull())
        .select("moeda", "data_cotacao", "valor_brl")
    )
