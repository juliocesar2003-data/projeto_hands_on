from pyspark import pipelines as dp

VOLUME_PATH = "/Volumes/projeto_hands_on/bronze/raw"


@dp.table(name="bronze_selic", comment="Taxa Selic meta do BACEN SGS código 432")
def bronze_selic():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "selic_*.json")
        .load(VOLUME_PATH)
    )


@dp.table(name="bronze_cdi", comment="Taxa CDI diária do BACEN SGS código 12")
def bronze_cdi():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "cdi_*.json")
        .load(VOLUME_PATH)
    )


@dp.table(name="bronze_tr", comment="Taxa TR diária do BACEN SGS código 226")
def bronze_tr():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "tr_*.json")
        .load(VOLUME_PATH)
    )


@dp.table(name="bronze_ipca", comment="Variação mensal do IPCA do BACEN SGS código 433")
def bronze_ipca():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "ipca_*.json")
        .load(VOLUME_PATH)
    )
