from pyspark import pipelines as dp

VOLUME_PATH = "/Volumes/projeto_hands_on/bronze/raw"


@dp.table(name="bronze_selic", comment="Raw Selic target rate from BACEN SGS code 432")
def bronze_selic():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "selic_*.json")
        .load(VOLUME_PATH)
    )


@dp.table(name="bronze_cdi", comment="Raw Daily CDI rate from BACEN SGS code 12")
def bronze_cdi():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "cdi_*.json")
        .load(VOLUME_PATH)
    )


@dp.table(name="bronze_tr", comment="Raw Daily TR rate from BACEN SGS code 226")
def bronze_tr():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "tr_*.json")
        .load(VOLUME_PATH)
    )


@dp.table(name="bronze_ipca", comment="Raw IPCA monthly variation from BACEN SGS code 433")
def bronze_ipca():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "ipca_*.json")
        .load(VOLUME_PATH)
    )
