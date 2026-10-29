# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Análise Exploratória de Dados Econômicos do BACEN
# MAGIC
# MAGIC ## Visão geral
# MAGIC
# MAGIC Este notebook apresenta a análise exploratória das séries econômicas do projeto **Hands-on BACEN**, lendo diretamente as tabelas da camada **silver** no Unity Catalog (`projeto_hands_on.silver`), alimentadas pelo pipeline do projeto:
# MAGIC
# MAGIC - USD/BRL, EUR/BRL, GBP/BRL, JPY/BRL e ARS/BRL (`silver.moedas_conversao`, diário);
# MAGIC - meta Selic (`silver.selic`, valor vigente no último dia do mês, % a.a.);
# MAGIC - CDI (`silver.cdi`, média mensal da taxa anualizada com 252 dias úteis, % a.a.);
# MAGIC - Taxa Referencial (`silver.tr`, diária);
# MAGIC - IPCA mensal (`silver.ipca`, % no mês).
# MAGIC
# MAGIC O período é definido pelos parâmetros `data_inicial` e `data_final` no topo do notebook. Por padrão, a análise vai de **01/2020 até o último mês completo**, sem o mês corrente, que ainda está parcial.
# MAGIC
# MAGIC ## Objetivos
# MAGIC
# MAGIC - avaliar a qualidade e a cobertura dos dados;
# MAGIC - explorar o comportamento das séries cambiais;
# MAGIC - comparar juros e inflação em unidades adequadas;
# MAGIC - analisar associações lineares entre as variáveis, em nível e em variação;
# MAGIC - avaliar a sazonalidade do IPCA;
# MAGIC - orientar a construção da camada gold e das features do modelo.
# MAGIC
# MAGIC > **Nota metodológica:** as análises são descritivas. Correlação não representa causalidade, e os resultados não constituem previsão ou recomendação financeira.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("data_inicial", "2020-01-01", "Data inicial (AAAA-MM-DD)")
dbutils.widgets.text("data_final", "", "Data final (AAAA-MM-DD, vazio = último mês completo)")

# COMMAND ----------

# DBTITLE 1,Environment
from datetime import date

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

pd.set_option("display.max_columns", 30)
pd.set_option("display.float_format", lambda valor: f"{valor:,.4f}")

plt.rcParams.update({
    "figure.figsize": (12, 6),
    "figure.dpi": 110,
    "axes.titlesize": 14,
    "axes.labelsize": 11,
    "axes.grid": True,
    "grid.alpha": 0.25,
    "legend.frameon": False,
})

CATALOGO = "projeto_hands_on.silver"

DATA_INICIAL = pd.Timestamp(dbutils.widgets.get("data_inicial"))
data_final_param = dbutils.widgets.get("data_final").strip()
# Sem data final, usa o último dia do mês anterior ao atual: o mês corrente ainda está incompleto
DATA_FINAL = pd.Timestamp(data_final_param) if data_final_param else pd.Timestamp(date.today().replace(day=1)) - pd.Timedelta(days=1)

print(f"Período analisado: {DATA_INICIAL:%d/%m/%Y} a {DATA_FINAL:%d/%m/%Y}")

# COMMAND ----------

# DBTITLE 1,Load
# MAGIC %md
# MAGIC ## 1. Carregamento
# MAGIC
# MAGIC As tabelas silver são lidas com Spark, filtradas pelo período e convertidas para pandas, pois o volume é pequeno (alguns milhares de linhas).

# COMMAND ----------

def ler_silver(tabela, coluna_data):
    """Lê uma tabela silver, filtra pelo período e devolve um DataFrame pandas com a coluna de data tipada."""
    df = (
        spark.table(f"{CATALOGO}.{tabela}")
        .where(f"{coluna_data} BETWEEN '{DATA_INICIAL:%Y-%m-%d}' AND '{DATA_FINAL:%Y-%m-%d}'")
        .toPandas()
    )
    df[coluna_data] = pd.to_datetime(df[coluna_data])
    return df


NOMES_MOEDAS = {"USD": "USD/BRL", "EUR": "EUR/BRL", "GBP": "GBP/BRL", "JPY": "JPY/BRL", "ARS": "ARS/BRL"}

cambio_diario = (
    ler_silver("moedas_conversao", "data_cotacao")
    .rename(columns={"data_cotacao": "data", "valor_brl": "valor"})
    .assign(serie=lambda df: df["moeda"].map(NOMES_MOEDAS))
    .sort_values(["serie", "data"])
    .reset_index(drop=True)
)
cambio_diario["valor"] = cambio_diario["valor"].astype(float)

selic = ler_silver("selic", "data_ref").astype({"valor": float})
cdi = ler_silver("cdi", "data_ref").astype({"valor": float, "acumulado_mes": float})
ipca = ler_silver("ipca", "data_ref").astype({"valor": float})
tr = ler_silver("tr", "data_ref").astype({"valor": float})

print("Registros cambiais:", len(cambio_diario))
print("Meses de Selic / CDI / IPCA:", len(selic), "/", len(cdi), "/", len(ipca))
print("Registros de TR:", len(tr))

# COMMAND ----------

# DBTITLE 1,Data quality
# MAGIC %md
# MAGIC ## 2. Qualidade e cobertura
# MAGIC
# MAGIC Verifica, para cada tabela, a cobertura temporal, os valores nulos e as chaves duplicadas. A TR pode ter mais de um registro por data de início (períodos com fim em datas diferentes), então sua chave é `data_ref` + `data_fim`.

# COMMAND ----------

def resumo_tabela(nome, df, coluna_data, chave):
    return {
        "tabela": nome,
        "registros": len(df),
        "data_inicial": df[coluna_data].min(),
        "data_final": df[coluna_data].max(),
        "nulos": int(df.drop(columns=chave).isna().sum().sum()),
        "chaves_duplicadas": int(df.duplicated(subset=chave).sum()),
    }


qualidade = pd.DataFrame([
    resumo_tabela("moedas_conversao", cambio_diario, "data", ["serie", "data"]),
    resumo_tabela("selic", selic, "data_ref", ["data_ref"]),
    resumo_tabela("cdi", cdi, "data_ref", ["data_ref"]),
    resumo_tabela("ipca", ipca, "data_ref", ["data_ref"]),
    resumo_tabela("tr", tr, "data_ref", ["data_ref", "data_fim"]),
])
display(qualidade)

if qualidade["chaves_duplicadas"].sum() > 0:
    print("Atenção: há chaves duplicadas; verifique a camada silver antes de usar os resultados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cobertura das séries cambiais

# COMMAND ----------

cobertura_cambio = (
    cambio_diario.groupby("serie", as_index=False)
    .agg(
        registros=("valor", "size"),
        data_inicial=("data", "min"),
        data_final=("data", "max"),
        nulos=("valor", lambda s: s.isna().sum()),
        menor_valor=("valor", "min"),
        maior_valor=("valor", "max"),
    )
)
display(cobertura_cambio)

# COMMAND ----------

# DBTITLE 1,Monthly base
# MAGIC %md
# MAGIC ## 3. Base econômica mensal
# MAGIC
# MAGIC Consolida as séries em uma linha por mês, com as seguintes regras:
# MAGIC
# MAGIC - **Selic:** valor vigente no último dia do mês (regra da silver);
# MAGIC - **CDI:** média mensal da taxa anualizada (`valor`, % a.a.) e taxa acumulada no mês (`acumulado_mes`, % no mês), ambas da silver;
# MAGIC - **TR:** média mensal das taxas com início no mês;
# MAGIC - **IPCA:** observação mensal;
# MAGIC - **Câmbio:** cotação do último dia útil do mês (fim de período), a mesma referência usada para medir se a moeda subiu ou caiu no mês.

# COMMAND ----------

tr_mensal = (
    tr.assign(mes=tr["data_ref"].dt.to_period("M").dt.to_timestamp())
    .groupby("mes", as_index=False)["valor"].mean()
    .rename(columns={"valor": "tr"})
)

cambio_mensal = (
    cambio_diario.pivot_table(index="data", columns="serie", values="valor")
    .resample("MS").last()
    .rename(columns=lambda serie: serie.lower().replace("/", "_"))
    .rename_axis(columns=None)
    .reset_index()
    .rename(columns={"data": "mes"})
)

base = (
    selic.rename(columns={"data_ref": "mes", "valor": "selic"})
    .merge(cdi.rename(columns={"data_ref": "mes", "valor": "cdi_anual_equivalente", "acumulado_mes": "cdi_acumulado_mes"})
           [["mes", "cdi_anual_equivalente", "cdi_acumulado_mes"]], on="mes", how="outer")
    .merge(tr_mensal, on="mes", how="outer")
    .merge(ipca.rename(columns={"data_ref": "mes", "valor": "ipca"}), on="mes", how="outer")
    .merge(cambio_mensal, on="mes", how="outer")
    .sort_values("mes")
    .reset_index(drop=True)
)

print("Período:", base["mes"].min().strftime("%m/%Y"), "a", base["mes"].max().strftime("%m/%Y"))
print("Meses:", len(base), "| meses duplicados:", base.duplicated(subset=["mes"]).sum())
display(pd.DataFrame({
    "coluna": base.columns,
    "tipo": base.dtypes.astype(str).values,
    "valores_validos": base.notna().sum().values,
    "valores_nulos": base.isna().sum().values,
}))

# COMMAND ----------

# DBTITLE 1,USD/BRL
# MAGIC %md
# MAGIC ## 4. Análise do USD/BRL
# MAGIC
# MAGIC A série USD/BRL é apresentada em granularidade diária para mostrar sua evolução histórica. Os extremos são identificados diretamente a partir dos dados da silver.

# COMMAND ----------

usd = cambio_diario.query("serie == 'USD/BRL'").sort_values("data").reset_index(drop=True)
usd["media_movel_30"] = usd["valor"].rolling(30).mean()

linha_min = usd.loc[usd["valor"].idxmin()]
linha_max = usd.loc[usd["valor"].idxmax()]

display(pd.DataFrame({
    "Indicador": [
        "Observações", "Data inicial", "Data final", "Média", "Mediana",
        "Mínimo", "Data do mínimo", "Máximo", "Data do máximo",
    ],
    "Resultado": [
        len(usd), usd["data"].min().strftime("%d/%m/%Y"),
        usd["data"].max().strftime("%d/%m/%Y"), f"R$ {usd['valor'].mean():.4f}",
        f"R$ {usd['valor'].median():.4f}", f"R$ {linha_min['valor']:.4f}",
        linha_min["data"].strftime("%d/%m/%Y"), f"R$ {linha_max['valor']:.4f}",
        linha_max["data"].strftime("%d/%m/%Y"),
    ],
}))

fig, ax = plt.subplots(figsize=(14, 6))
ax.plot(usd["data"], usd["valor"], linewidth=1, alpha=0.6, label="USD/BRL diário")
ax.plot(usd["data"], usd["media_movel_30"], linewidth=2.2, label="Média móvel (30 observações)")
ax.scatter(linha_min["data"], linha_min["valor"], color="#2a9d8f", s=65, zorder=3, label="Mínimo")
ax.scatter(linha_max["data"], linha_max["valor"], color="#e76f51", s=65, zorder=3, label="Máximo")
ax.set(title=f"Evolução do USD/BRL — {DATA_INICIAL:%m/%Y} a {DATA_FINAL:%m/%Y}", xlabel="Data", ylabel="Cotação em BRL")
ax.legend(ncol=2)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Leitura:** a média móvel reduz o ruído diário e evidencia movimentos mais persistentes. Os pontos mínimo e máximo representam extremos do período, não tendências isoladamente.

# COMMAND ----------

# DBTITLE 1,Currencies
# MAGIC %md
# MAGIC ## 5. Comparação das cinco moedas
# MAGIC
# MAGIC Como as cotações possuem escalas distintas, a comparação utiliza um índice Base 100. Cada série começa em 100, possibilitando comparar a evolução relativa sem confundir os valores absolutos.

# COMMAND ----------

cambio_base100 = cambio_diario.copy()
cambio_base100["indice_base_100"] = (
    cambio_base100["valor"] / cambio_base100.groupby("serie")["valor"].transform("first") * 100
)

fig, ax = plt.subplots(figsize=(14, 7))
for serie, dados in cambio_base100.groupby("serie"):
    ax.plot(dados["data"], dados["indice_base_100"], linewidth=1.5, label=serie)
ax.axhline(100, color="black", linestyle="--", linewidth=1, label="Base inicial")
ax.set(title="Evolução relativa das séries cambiais — Base 100", xlabel="Data", ylabel="Índice Base 100")
ax.legend(ncol=3)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variação acumulada e volatilidade
# MAGIC
# MAGIC A variação acumulada compara a primeira e a última observação. A volatilidade corresponde ao desvio padrão das variações percentuais entre observações consecutivas.

# COMMAND ----------

variacao_cambio = (
    cambio_diario.groupby("serie")
    .agg(data_inicial=("data", "first"), valor_inicial=("valor", "first"),
         data_final=("data", "last"), valor_final=("valor", "last"))
    .assign(variacao_acumulada_pct=lambda df: (df["valor_final"] / df["valor_inicial"] - 1) * 100)
    .reset_index()
)

variacoes_diarias = cambio_diario.assign(
    variacao_pct=cambio_diario.groupby("serie")["valor"].pct_change(fill_method=None) * 100
)
volatilidade_cambio = (
    variacoes_diarias.groupby("serie")["variacao_pct"]
    .agg(volatilidade_pct="std", menor_variacao_pct="min", maior_variacao_pct="max")
    .reset_index()
)

variacao_plot = variacao_cambio.sort_values("variacao_acumulada_pct")
vol_plot = volatilidade_cambio.sort_values("volatilidade_pct")

fig, axes = plt.subplots(1, 2, figsize=(15, 6))
cores = ["#2a9d8f" if v >= 0 else "#e76f51" for v in variacao_plot["variacao_acumulada_pct"]]
axes[0].barh(variacao_plot["serie"], variacao_plot["variacao_acumulada_pct"], color=cores)
axes[0].axvline(0, color="black", linestyle="--", linewidth=1)
axes[0].set(title="Variação acumulada", xlabel="Variação (%)", ylabel="Série")
axes[1].barh(vol_plot["serie"], vol_plot["volatilidade_pct"], color="#f4a261")
axes[1].set(title="Volatilidade diária", xlabel="Desvio padrão das variações (%)", ylabel="Série")
fig.suptitle(f"Comparação das séries cambiais — {DATA_INICIAL:%m/%Y} a {DATA_FINAL:%m/%Y}", fontsize=15)
fig.tight_layout()
plt.show()

display(variacao_cambio[["serie", "valor_inicial", "valor_final", "variacao_acumulada_pct"]].round(4))
display(volatilidade_cambio.round(4))

# COMMAND ----------

# DBTITLE 1,Economic indicators
# MAGIC %md
# MAGIC ## 6. Indicadores econômicos

# COMMAND ----------

display(base[["selic", "cdi_anual_equivalente", "cdi_acumulado_mes", "tr", "ipca"]].describe().T)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selic e CDI anual equivalente
# MAGIC
# MAGIC As duas séries são comparadas em percentual ao ano. A diferença entre elas é expressa em pontos percentuais.

# COMMAND ----------

comparacao_juros = base[["mes", "selic", "cdi_anual_equivalente"]].dropna().copy()
comparacao_juros["diferenca_pp"] = comparacao_juros["selic"] - comparacao_juros["cdi_anual_equivalente"]

fig, axes = plt.subplots(2, 1, figsize=(14, 9), sharex=True)
axes[0].plot(comparacao_juros["mes"], comparacao_juros["selic"], linewidth=2, label="Selic")
axes[0].plot(comparacao_juros["mes"], comparacao_juros["cdi_anual_equivalente"], linewidth=2, label="CDI anual equivalente")
axes[0].set(title="Selic e CDI anual equivalente", ylabel="Taxa (% ao ano)")
axes[0].legend()
axes[1].plot(comparacao_juros["mes"], comparacao_juros["diferenca_pp"], color="#7b2cbf", linewidth=1.7)
axes[1].axhline(0, color="black", linestyle="--", linewidth=1)
axes[1].set(title="Diferença Selic − CDI", xlabel="Mês", ylabel="Pontos percentuais")
fig.tight_layout()
plt.show()

display(comparacao_juros["diferenca_pp"].describe().to_frame("diferença em p.p.").T)

# COMMAND ----------

# MAGIC %md
# MAGIC **Leitura:** o CDI acompanha a Selic de perto, normalmente alguns centésimos abaixo. Os picos da diferença aparecem nos meses com decisão do Copom: a Selic considera a taxa vigente no fim do mês, enquanto o CDI é a média do mês, que mistura a taxa antiga e a nova.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selic e IPCA
# MAGIC
# MAGIC A Selic é uma taxa anual e o IPCA representa variação mensal. Por isso, as séries são apresentadas em painéis separados e não são subtraídas diretamente. Para comparar juros e inflação na mesma unidade, usa-se o CDI acumulado no mês contra o IPCA do mês (juro real aproximado).

# COMMAND ----------

comparacao_inflacao = base[["mes", "selic", "ipca", "cdi_acumulado_mes"]].dropna().copy()
comparacao_inflacao["juro_real_pp"] = comparacao_inflacao["cdi_acumulado_mes"] - comparacao_inflacao["ipca"]
correlacao_selic_ipca = comparacao_inflacao["selic"].corr(comparacao_inflacao["ipca"])

fig, axes = plt.subplots(3, 1, figsize=(14, 12), sharex=True)
axes[0].plot(comparacao_inflacao["mes"], comparacao_inflacao["selic"], color="#1f77b4", linewidth=2)
axes[0].set(title="Meta Selic", ylabel="% ao ano")
axes[1].plot(comparacao_inflacao["mes"], comparacao_inflacao["ipca"], color="#e76f51", linewidth=2, marker="o", markersize=3)
axes[1].axhline(0, color="black", linestyle="--", linewidth=1)
axes[1].set(title="IPCA", ylabel="% no mês")
axes[2].bar(comparacao_inflacao["mes"], comparacao_inflacao["juro_real_pp"], width=20,
            color=["#2a9d8f" if v >= 0 else "#e76f51" for v in comparacao_inflacao["juro_real_pp"]])
axes[2].axhline(0, color="black", linestyle="--", linewidth=1)
axes[2].set(title="CDI acumulado no mês − IPCA (juro real aproximado)", xlabel="Mês", ylabel="p.p. no mês")
fig.suptitle("Evolução mensal da Selic, do IPCA e do juro real", fontsize=15)
fig.tight_layout()
plt.show()

print(f"Correlação contemporânea Selic × IPCA: {correlacao_selic_ipca:.4f}")
print(f"Meses com juro real negativo: {(comparacao_inflacao['juro_real_pp'] < 0).sum()} de {len(comparacao_inflacao)}")
print("A correlação é descritiva e não demonstra causalidade.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Defasagem entre Selic e IPCA
# MAGIC
# MAGIC A política monetária reage à inflação com atraso, e a inflação reage aos juros com atraso ainda maior. A correlação contemporânea, portanto, diz pouco. Abaixo, a correlação da Selic com o IPCA de *k* meses antes.

# COMMAND ----------

defasagens = pd.DataFrame({
    "defasagem_meses": range(0, 13),
    "correlacao": [comparacao_inflacao["selic"].corr(comparacao_inflacao["ipca"].shift(k)) for k in range(0, 13)],
})

fig, ax = plt.subplots(figsize=(12, 5))
ax.bar(defasagens["defasagem_meses"], defasagens["correlacao"], color="#1f77b4")
ax.axhline(0, color="black", linewidth=1)
ax.set(title="Correlação da Selic com o IPCA de k meses antes", xlabel="k (meses)", ylabel="Correlação")
fig.tight_layout()
plt.show()

display(defasagens.round(4))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Taxa Referencial

# COMMAND ----------

fig, ax = plt.subplots(figsize=(14, 5))
ax.plot(tr_mensal["mes"], tr_mensal["tr"], color="#2a9d8f", linewidth=2, marker="o", markersize=3)
ax.axhline(0, color="black", linestyle="--", linewidth=1)
ax.set(title="Evolução mensal da Taxa Referencial", xlabel="Mês", ylabel="TR média mensal (% no período)")
fig.tight_layout()
plt.show()

display(tr_mensal["tr"].describe().to_frame("TR mensal").T)
print("Valores zero de TR são observações válidas: a TR é calculada como zero quando a Selic está baixa.")

# COMMAND ----------

# DBTITLE 1,Seasonality
# MAGIC %md
# MAGIC ## 7. Sazonalidade do IPCA
# MAGIC
# MAGIC Agrupa o IPCA por mês do calendário para identificar meses de inflação tipicamente maior ou menor (por exemplo, reajustes de início de ano e efeito de alimentos e energia).

# COMMAND ----------

NOMES_MESES = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]

sazonalidade = base[["mes", "ipca"]].dropna().assign(mes_calendario=lambda df: df["mes"].dt.month)
media_por_mes = sazonalidade.groupby("mes_calendario")["ipca"].agg(["mean", "median", "count"]).reindex(range(1, 13))

fig, ax = plt.subplots(figsize=(13, 6))
ax.boxplot([sazonalidade.loc[sazonalidade["mes_calendario"] == m, "ipca"] for m in range(1, 13)])
ax.set_xticks(range(1, 13), NOMES_MESES)
ax.plot(range(1, 13), media_por_mes["mean"], color="#e76f51", marker="o", linewidth=1.5, label="Média")
ax.axhline(sazonalidade["ipca"].mean(), color="black", linestyle="--", linewidth=1, label="Média geral")
ax.set(title="IPCA por mês do calendário", xlabel="Mês", ylabel="IPCA (% no mês)")
ax.legend()
fig.tight_layout()
plt.show()

display(media_por_mes.rename(index=dict(enumerate(NOMES_MESES, start=1)), columns={"mean": "media", "median": "mediana", "count": "anos"}).round(4))

# COMMAND ----------

# DBTITLE 1,Correlation
# MAGIC %md
# MAGIC ## 8. Matriz de correlação
# MAGIC
# MAGIC A matriz de Pearson mede associação linear contemporânea. Valores próximos de 1 indicam associação positiva; valores próximos de -1 indicam associação negativa; valores próximos de zero indicam baixa associação linear.
# MAGIC
# MAGIC > Séries temporais podem apresentar correlações elevadas devido a tendências compartilhadas. Por isso, a matriz é calculada duas vezes: **em nível** (valores) e **em variação mensal** (mudança de um mês para o outro), que remove boa parte do efeito de tendência e é a forma relevante para um modelo preditivo.

# COMMAND ----------

ROTULOS = {
    "selic": "Selic", "cdi_anual_equivalente": "CDI anual", "tr": "TR", "ipca": "IPCA",
    "usd_brl": "USD/BRL", "eur_brl": "EUR/BRL", "gbp_brl": "GBP/BRL", "jpy_brl": "JPY/BRL", "ars_brl": "ARS/BRL",
}
MOEDAS = ["usd_brl", "eur_brl", "gbp_brl", "jpy_brl", "ars_brl"]
TAXAS = ["selic", "cdi_anual_equivalente", "tr"]

nivel = base[list(ROTULOS)].rename(columns=ROTULOS)
# Moedas em variação percentual; taxas em variação absoluta (p.p.); IPCA já é uma variação mensal
variacao = pd.concat([
    base[MOEDAS].pct_change(fill_method=None) * 100,
    base[TAXAS].diff(),
    base[["ipca"]],
], axis=1)[list(ROTULOS)].rename(columns=ROTULOS)


def desenhar_matriz(matriz, titulo, ax):
    imagem = ax.imshow(matriz.values, cmap="coolwarm", vmin=-1, vmax=1)
    ax.set_xticks(range(len(matriz.columns)), matriz.columns, rotation=45, ha="right")
    ax.set_yticks(range(len(matriz.index)), matriz.index)
    for i in range(len(matriz.index)):
        for j in range(len(matriz.columns)):
            ax.text(j, i, f"{matriz.iat[i, j]:.2f}", ha="center", va="center", fontsize=8)
    ax.set_title(titulo)
    ax.grid(False)
    return imagem


matriz_nivel = nivel.corr()
matriz_variacao = variacao.corr()

fig, axes = plt.subplots(1, 2, figsize=(20, 9))
desenhar_matriz(matriz_nivel, "Em nível", axes[0])
imagem = desenhar_matriz(matriz_variacao, "Em variação mensal", axes[1])
fig.colorbar(imagem, ax=axes, shrink=0.8, label="Coeficiente de correlação")
fig.suptitle("Matriz de correlação — indicadores e séries cambiais", fontsize=15)
plt.show()

# COMMAND ----------

def principais_pares(matriz, n=10):
    mascara_superior = np.triu(np.ones(matriz.shape, dtype=bool), k=1)
    pares = matriz.where(mascara_superior).stack().reset_index()
    pares.columns = ["variavel_1", "variavel_2", "correlacao"]
    pares["correlacao_absoluta"] = pares["correlacao"].abs()
    return pares.sort_values("correlacao_absoluta", ascending=False).head(n).reset_index(drop=True)


print("Em nível")
display(principais_pares(matriz_nivel).round(4))
print("Em variação mensal")
display(principais_pares(matriz_variacao).round(4))

# COMMAND ----------

# DBTITLE 1,Conclusions
# MAGIC %md
# MAGIC ## 9. Principais conclusões
# MAGIC
# MAGIC - A análise lê as tabelas silver do pipeline, então os números acompanham a ingestão diária e são reproduzíveis por qualquer pessoa com acesso ao workspace.
# MAGIC - O USD/BRL apresentou oscilações diárias que ficam mais claras quando observadas em conjunto com a média móvel.
# MAGIC - A comparação Base 100 permitiu analisar as trajetórias relativas das cinco moedas sem misturar suas escalas monetárias.
# MAGIC - Variação acumulada e volatilidade representam dimensões diferentes: posição final em relação ao início e intensidade das oscilações, respectivamente.
# MAGIC - Selic e CDI só são comparáveis porque o CDI diário é anualizado na silver; juros e inflação são comparados na mesma unidade pelo CDI acumulado no mês contra o IPCA.
# MAGIC - A relação entre Selic e IPCA aparece com defasagem, não no mesmo mês.
# MAGIC - Várias correlações altas em nível diminuem bastante em variação mensal: parte delas vem de tendências compartilhadas, não de movimento conjunto mês a mês.
# MAGIC - A TR foi tratada considerando que valores zero podem ser observações válidas.
# MAGIC
# MAGIC ## Limitações
# MAGIC
# MAGIC - período padrão restrito a 2020 em diante (ajustável pelos parâmetros);
# MAGIC - ausência de variáveis externas e expectativas de mercado;
# MAGIC - uso de correlação de Pearson, restrita a associações lineares;
# MAGIC - ausência de testes econométricos e finalidade preditiva;
# MAGIC - resultados não constituem recomendação financeira.
# MAGIC
# MAGIC ## Implicações para a camada gold e o modelo
# MAGIC
# MAGIC 1. Usar granularidade mensal, com o câmbio de fim de mês e variações percentuais (não níveis) como features.
# MAGIC 2. Usar o IPCA com 1 mês de defasagem, pois ele é publicado por volta do dia 10 do mês seguinte.
# MAGIC 3. Considerar defasagens da Selic e do IPCA como features, dado o efeito com atraso observado.
# MAGIC 4. Separar treino e teste por tempo, sem embaralhar os meses.
