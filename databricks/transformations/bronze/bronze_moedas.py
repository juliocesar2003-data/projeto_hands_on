from pyspark import pipelines as dp
from pyspark.sql.functions import lit

VOLUME_PATH = "/Volumes/projeto_hands_on/bronze/raw"

dp.create_streaming_table(
    name="bronze_moedas",
    comment="Dados brutos de cotações de câmbio da API do BACEN (5 moedas: EUR, USD, GBP, JPY, ARS)",
    schema="data STRING, valor STRING, moeda_arquivo STRING",
    # Apenas registram violações: a bronze guarda o dado bruto como veio da API
    expect_all={
        "data_valida": "try_to_date(data, 'dd/MM/yyyy') IS NOT NULL",
        "valor_numerico": "try_cast(valor AS DOUBLE) IS NOT NULL",
        "cotacao_positiva": "try_cast(valor AS DOUBLE) > 0",
    },
)


@dp.append_flow(target="bronze_moedas", name="euro_flow")
def euro_flow():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "euro_*.json")
        .load(VOLUME_PATH)
        .select("data", "valor")
        .withColumn("moeda_arquivo", lit("euro"))
    )


@dp.append_flow(target="bronze_moedas", name="dolar_flow")
def dolar_flow():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "dolar_*.json")
        .load(VOLUME_PATH)
        .select("data", "valor")
        .withColumn("moeda_arquivo", lit("dolar"))
    )


@dp.append_flow(target="bronze_moedas", name="libra_flow")
def libra_flow():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "libra_*.json")
        .load(VOLUME_PATH)
        .select("data", "valor")
        .withColumn("moeda_arquivo", lit("libra"))
    )


@dp.append_flow(target="bronze_moedas", name="iene_flow")
def iene_flow():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "iene_*.json")
        .load(VOLUME_PATH)
        .select("data", "valor")
        .withColumn("moeda_arquivo", lit("iene"))
    )


@dp.append_flow(target="bronze_moedas", name="peso_argentino_flow")
def peso_argentino_flow():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "peso_argentino_*.json")
        .load(VOLUME_PATH)
        .select("data", "valor")
        .withColumn("moeda_arquivo", lit("peso_argentino"))
    )
