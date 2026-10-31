from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

VOLUME_PATH = "/Volumes/projeto_hands_on/bronze/raw"

# Regras de qualidade da bronze: apenas registram violações (a bronze guarda o dado bruto como veio da API)
REGRAS_BRONZE = {
    "data_valida": "try_to_date(data, 'dd/MM/yyyy') IS NOT NULL",
    "valor_numerico": "try_cast(valor AS DOUBLE) IS NOT NULL",
    "origem_ingestao_preenchida": "origem_ingestao IS NOT NULL",
}


def ler_arquivos(padrao):
    """Lê os arquivos NDJSON do volume que casam com o padrão, com a linhagem de cada registro: arquivo de origem
    no volume, quando o arquivo foi gravado pela ingestão (não muda se a bronze for reprocessada) e quando o
    pipeline leu o registro."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", padrao)
        .load(VOLUME_PATH)
        .withColumn("origem_ingestao", col("_metadata.file_path"))
        .withColumn("data_ingestao", col("_metadata.file_modification_time"))
        .withColumn("data_processamento", current_timestamp())
    )


@dp.table(name="bronze_selic", comment="Taxa Selic meta do BACEN SGS código 432")
@dp.expect_all(REGRAS_BRONZE)
def bronze_selic():
    return ler_arquivos("selic_*.json")


@dp.table(name="bronze_cdi", comment="Taxa CDI diária do BACEN SGS código 12")
@dp.expect_all(REGRAS_BRONZE)
def bronze_cdi():
    return ler_arquivos("cdi_*.json")


@dp.table(name="bronze_tr", comment="Taxa TR diária do BACEN SGS código 226")
@dp.expect_all({
    **REGRAS_BRONZE,
    "periodo_valido": "try_to_date(dataFim, 'dd/MM/yyyy') > try_to_date(data, 'dd/MM/yyyy')",
})
def bronze_tr():
    return ler_arquivos("tr_*.json")


@dp.table(name="bronze_ipca", comment="Variação mensal do IPCA do BACEN SGS código 433")
@dp.expect_all(REGRAS_BRONZE)
def bronze_ipca():
    return ler_arquivos("ipca_*.json")
