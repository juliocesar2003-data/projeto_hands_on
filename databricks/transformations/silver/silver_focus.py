from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit, to_date


def expectativas(tabela, fonte):
    return (
        spark.read.table(tabela)
        .select(
            lit(fonte).alias("fonte"),
            to_date(col("Data")).alias("data_pesquisa"),
            to_date(col("DataReferencia"), "MM/yyyy").alias("mes_referencia"),
            col("Mediana").cast("double").alias("mediana"),
            col("Media").cast("double").alias("media"),
        )
        .dropDuplicates(["fonte", "data_pesquisa", "mes_referencia"])
    )


@dp.materialized_view(
    name="projeto_hands_on.silver.focus_ipca",
    comment=(
        "Expectativas de mercado para o IPCA mensal (% no mês): uma linha por fonte (focus ou top5), data da pesquisa "
        "e mês de referência. Permite consultar o que o mercado esperava em qualquer data passada"
    ),
)
@dp.expect_all_or_fail({
    "data_pesquisa_valida": "data_pesquisa IS NOT NULL",
    "mes_referencia_valido": "mes_referencia IS NOT NULL",
})
# Expectativas muito altas ocorreram no início dos anos 2000; fora desta faixa, apenas registra para investigação.
# Mediana vazia é válida: o Top 5 tem 328 registros assim de 2001 a 2010, já registrados na bronze
@dp.expect("faixa_plausivel", "mediana IS NULL OR mediana BETWEEN -5 AND 20")
def focus_ipca():
    return expectativas("bronze_focus_mensal", "focus").unionByName(expectativas("bronze_focus_top5", "top5"))
