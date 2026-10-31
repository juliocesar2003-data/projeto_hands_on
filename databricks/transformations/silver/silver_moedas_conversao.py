from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date, when


@dp.materialized_view(
    name="projeto_hands_on.silver.moedas_conversao",
    comment="Cotações de câmbio consolidadas e tipadas (EUR, USD, GBP, JPY, ARS)",
)
# Moeda e data sem valor indicam falha no mapeamento ou no formato do arquivo: interrompe a atualização
@dp.expect_all_or_fail({
    "moeda_conhecida": "moeda IS NOT NULL",
    "data_cotacao_valida": "data_cotacao IS NOT NULL",
})
# Cotação zerada ou negativa não é utilizável: a linha é descartada e contabilizada nas métricas
@dp.expect_or_drop("cotacao_positiva", "valor_brl > 0")
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
