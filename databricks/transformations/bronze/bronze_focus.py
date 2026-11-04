from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

VOLUME_FOCUS = "/Volumes/projeto_hands_on/bronze/raw_focus"

# Regras de qualidade da bronze: apenas registram violações (a bronze guarda o dado bruto como veio da API)
REGRAS_FOCUS = {
    "data_valida": "try_to_date(Data) IS NOT NULL",
    "data_referencia_valida": "try_to_date(DataReferencia, 'MM/yyyy') IS NOT NULL",
    "mediana_numerica": "try_cast(Mediana AS DOUBLE) IS NOT NULL",
    "origem_ingestao_preenchida": "origem_ingestao IS NOT NULL",
}


def ler_focus(padrao):
    """Lê os arquivos do Focus no volume que casam com o padrão, com a mesma linhagem das demais tabelas bronze."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", padrao)
        .load(VOLUME_FOCUS)
        .withColumn("origem_ingestao", col("_metadata.file_path"))
        .withColumn("data_ingestao", col("_metadata.file_modification_time"))
        .withColumn("data_processamento", current_timestamp())
    )


@dp.table(name="bronze_focus_mensal", comment="Boletim Focus: expectativas mensais de IPCA de todas as instituições (API de Expectativas do BCB)")
@dp.expect_all(REGRAS_FOCUS)
def bronze_focus_mensal():
    return ler_focus("focus_mensal_*.json")


@dp.table(name="bronze_focus_top5", comment="Boletim Focus Top 5: expectativas mensais de IPCA do ranking de curto prazo (API de Expectativas do BCB)")
@dp.expect_all(REGRAS_FOCUS)
def bronze_focus_top5():
    return ler_focus("focus_top5_*.json")
