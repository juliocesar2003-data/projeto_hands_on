from pyspark import pipelines as dp

VOLUME_PATH = "/Volumes/projeto_hands_on/bronze/raw"

# Regras de qualidade da bronze: apenas registram violações (a bronze guarda o dado bruto como veio da API)
REGRAS_BRONZE = {
    "data_valida": "try_to_date(data, 'dd/MM/yyyy') IS NOT NULL",
    "valor_numerico": "try_cast(valor AS DOUBLE) IS NOT NULL",
}


@dp.table(name="bronze_selic", comment="Taxa Selic meta do BACEN SGS código 432")
@dp.expect_all(REGRAS_BRONZE)
def bronze_selic():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "selic_*.json")
        .load(VOLUME_PATH)
    )


@dp.table(name="bronze_cdi", comment="Taxa CDI diária do BACEN SGS código 12")
@dp.expect_all(REGRAS_BRONZE)
def bronze_cdi():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "cdi_*.json")
        .load(VOLUME_PATH)
    )


@dp.table(name="bronze_tr", comment="Taxa TR diária do BACEN SGS código 226")
@dp.expect_all({
    **REGRAS_BRONZE,
    "periodo_valido": "try_to_date(dataFim, 'dd/MM/yyyy') > try_to_date(data, 'dd/MM/yyyy')",
})
def bronze_tr():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "tr_*.json")
        .load(VOLUME_PATH)
    )


@dp.table(name="bronze_ipca", comment="Variação mensal do IPCA do BACEN SGS código 433")
@dp.expect_all(REGRAS_BRONZE)
def bronze_ipca():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "ipca_*.json")
        .load(VOLUME_PATH)
    )
