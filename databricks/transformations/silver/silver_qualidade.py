from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, countDistinct, current_date, current_timestamp, datediff, lit, max as spark_max

# Tabela silver -> (chave que deve ser única, coluna de data, atraso máximo aceitável em dias).
# Séries diárias: até 7 dias sem dado (feriados prolongados como o Carnaval chegam a 5).
# Selic e CDI mensais: datadas no dia 1 do mês, então a última data tem até 31 dias + 7 de tolerância.
# IPCA: o mês M é datado no dia 1 e publicado por volta do dia 10 de M+1, então o atraso chega a ~70 dias.
TABELAS = {
    "moedas_conversao": (["moeda", "data_cotacao"], "data_cotacao", 7),
    "selic": (["data_ref"], "data_ref", 38),
    "cdi": (["data_ref"], "data_ref", 38),
    "tr": (["data_ref", "data_fim"], "data_ref", 7),
    "ipca": (["data_ref"], "data_ref", 75),
}


@dp.materialized_view(
    name="projeto_hands_on.silver.dq_resumo",
    comment=(
        "Resumo de qualidade das tabelas silver, recalculado a cada atualização do pipeline: "
        "linhas, chaves distintas (unicidade), última data e dias desde a última data (atualização)"
    ),
)
# Chave duplicada indica erro de carga (como os meses repetidos do IPCA): interrompe a atualização
@dp.expect_or_fail("chave_unica", "linhas = chaves_distintas")
# Dado desatualizado indica falha na ingestão diária: apenas registra, pois a próxima execução recupera o período
@dp.expect("dados_atualizados", "dias_desde_ultima_data <= limite_atraso_dias")
def dq_resumo():
    resumos = [
        spark.read.table(f"projeto_hands_on.silver.{tabela}")
        .agg(
            count("*").alias("linhas"),
            countDistinct(*[col(c) for c in chave]).alias("chaves_distintas"),
            spark_max(coluna_data).alias("ultima_data"),
        )
        .select(
            lit(tabela).alias("tabela"),
            "linhas",
            "chaves_distintas",
            "ultima_data",
            datediff(current_date(), col("ultima_data")).alias("dias_desde_ultima_data"),
            lit(limite).alias("limite_atraso_dias"),
            current_timestamp().alias("verificado_em"),
        )
        for tabela, (chave, coluna_data, limite) in TABELAS.items()
    ]
    return reduce(lambda a, b: a.unionByName(b), resumos)
