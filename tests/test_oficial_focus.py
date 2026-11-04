"""Tests for the Focus ingestion and the official model: no future surveys and no future targets."""

from urllib.parse import quote, urlencode

import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from conftest import NOTEBOOKS, TRANSFORMATIONS, carregar_constante, carregar_funcoes

TREINO = NOTEBOOKS / "treino_modelo_ipca.py"
INGESTAO = NOTEBOOKS / "ingestao_focus.py"
HORIZONTES = list(range(1, 13))


@pytest.fixture
def f():
    return carregar_funcoes(
        TREINO,
        "compor", "indexar_expectativas", "expectativa_em", "limite_pesquisa", "linhas_mercado", "nova_correcao",
        "ajustar_correcao", "media_corrigida",
        np=np, pd=pd, Ridge=Ridge, make_pipeline=make_pipeline, StandardScaler=StandardScaler, HORIZONTES=HORIZONTES,
        MAXIMO_DIAS_PESQUISA=45, JANELA_REVISAO_DIAS=28, ALPHA_CORRECAO=50.0, ALVOS_CORRIGIDOS=["h1"],
    )


@pytest.fixture
def ingestao():
    return carregar_funcoes(INGESTAO, "montar_filtro", "buscar_pagina", "buscar_registros", "filtrar_novos", HEADERS={},
                            URL_BASE="http://exemplo/", TAMANHO_PAGINA=2, urlencode=urlencode, quote=quote)


def _pesquisas(linhas):
    df = pd.DataFrame(linhas, columns=["data_pesquisa", "mes_referencia", "mediana"])
    for c in ("data_pesquisa", "mes_referencia"):
        df[c] = pd.to_datetime(df[c])
    return df


def test_usa_a_ultima_pesquisa_ate_o_limite_e_ignora_as_seguintes(f):
    indice = f["indexar_expectativas"](_pesquisas([
        ("2010-12-03", "2011-01-01", 0.60), ("2011-01-05", "2011-01-01", 0.61), ("2011-02-07", "2011-01-01", 0.78),
    ]))
    ref = pd.Timestamp("2011-01-01")
    assert f["expectativa_em"](indice, ref, pd.Timestamp("2011-01-05")) == 0.61
    assert f["expectativa_em"](indice, ref, pd.Timestamp("2011-01-04")) == 0.60
    assert np.isnan(f["expectativa_em"](indice, ref, pd.Timestamp("2010-12-01")))


def test_pesquisa_antiga_demais_nao_vale(f):
    indice = f["indexar_expectativas"](_pesquisas([("2010-01-01", "2011-01-01", 0.50)]))
    assert np.isnan(f["expectativa_em"](indice, pd.Timestamp("2011-01-01"), pd.Timestamp("2010-06-01")))


def test_limite_e_o_dia_5_do_mes_seguinte_ao_mes_base(f):
    assert f["limite_pesquisa"](pd.Timestamp("2010-12-01")) == pd.Timestamp("2011-01-05")


def _mercado(f, top5_sem_h2=False):
    base = pd.Timestamp("2020-01-01")
    meses = [base + pd.DateOffset(months=h) for h in HORIZONTES]
    focus = _pesquisas([("2020-02-03", m, 0.3) for m in meses])
    top5 = _pesquisas([("2020-01-06", m, 0.2) for m in meses] + [("2020-02-03", m, 0.4) for m in meses])
    if top5_sem_h2:
        top5 = top5[top5["mes_referencia"] != meses[1]]
    ipca = pd.Series(0.5, index=meses[:3])
    return f["linhas_mercado"](base, np.full(12, 0.5), f["indexar_expectativas"](focus), f["indexar_expectativas"](top5), ipca,
                               pd.Timestamp("2020-02-05"))


def test_linhas_mercado_tem_revisao_de_4_semanas_e_alvo_so_quando_conhecido(f):
    linhas = {l["alvo"]: l for l in _mercado(f)}
    assert set(linhas) == {*(f"h{h}" for h in HORIZONTES), "acum_12m"}
    assert linhas["h1"]["revisao_top5"] == pytest.approx(0.2)
    assert linhas["h1"]["y"] == 0.5 and np.isnan(linhas["h4"]["y"])
    assert np.isnan(linhas["acum_12m"]["y"])
    assert linhas["acum_12m"]["focus"] == pytest.approx((1.003**12 - 1) * 100)


def test_top5_ausente_usa_o_focus_sem_inventar_revisao(f):
    linhas = {l["alvo"]: l for l in _mercado(f, top5_sem_h2=True)}
    assert linhas["h2"]["top5"] == 0.3
    assert np.isnan(linhas["h2"]["revisao_top5"])


def _painel(n=120):
    rng = np.random.default_rng(3)
    bases = pd.date_range("2005-01-01", periods=n, freq="MS")
    revisao = rng.normal(0, 0.1, n)
    media = rng.normal(0.4, 0.2, n)
    y = media + 0.3 * revisao + rng.normal(0, 0.01, n)
    h1 = pd.DataFrame({"alvo": "h1", "mes_base": bases, "mes_previsto": bases + pd.DateOffset(months=1), "y": y,
                       "media_tres": media, "revisao_top5": revisao})
    return pd.concat([h1, h1.assign(alvo="h2", mes_previsto=bases + pd.DateOffset(months=2))], ignore_index=True)


def test_correcao_so_altera_os_alvos_corrigidos_e_melhora_quando_ha_sinal(f):
    painel = _painel()
    resultado = f["media_corrigida"](painel, [2010, 2011, 2012, 2013, 2014])
    h2 = painel["alvo"] == "h2"
    assert (resultado[h2] == painel.loc[h2, "media_tres"]).all()
    teste = (painel["alvo"] == "h1") & (painel["mes_base"].dt.year >= 2010)
    assert (painel.loc[teste, "y"] - resultado[teste]).abs().mean() < (painel.loc[teste, "y"] - painel.loc[teste, "media_tres"]).abs().mean()


def test_correcao_de_um_ano_nao_usa_alvos_desse_ano_em_diante(f):
    painel = _painel()
    original = f["media_corrigida"](painel, [2010])
    alterado = painel.copy()
    # Mudar qualquer valor real a partir de 2010 não pode alterar a previsão de 2010
    alterado.loc[alterado["mes_previsto"] >= "2010-01-01", "y"] += 100
    novo = f["media_corrigida"](alterado, [2010])
    teste = (painel["alvo"] == "h1") & (painel["mes_base"].dt.year == 2010)
    np.testing.assert_allclose(original[teste], novo[teste])


def test_filtro_e_registros_novos_da_ingestao(ingestao):
    assert ingestao["montar_filtro"]("A eq 1", None) == "A eq 1"
    assert ingestao["montar_filtro"]("A eq 1", "2026-10-30") == "A eq 1 and Data gt '2026-10-30'"
    registros = [{"Data": "2026-10-30", "x": 1}, {"Data": "2026-11-03", "x": 1}, {"Data": "2026-11-03", "x": 1}, {"Data": "2026-11-03", "x": 2}]
    assert ingestao["filtrar_novos"](registros, "2026-10-30") == [{"Data": "2026-11-03", "x": 1}, {"Data": "2026-11-03", "x": 2}]
    assert len(ingestao["filtrar_novos"](registros, None)) == 3


def test_ingestao_percorre_todas_as_paginas_e_avisa_falha(ingestao, monkeypatch):
    paginas = iter([[1, 2], [3]])
    monkeypatch.setitem(ingestao, "buscar_pagina", lambda url, params: next(paginas))
    assert ingestao["buscar_registros"]("recurso", "filtro") == [1, 2, 3]
    monkeypatch.setitem(ingestao, "buscar_pagina", lambda url, params: None)
    assert ingestao["buscar_registros"]("recurso", "filtro") is None


def test_consulta_usa_espaco_codificado_e_nao_mais(ingestao, monkeypatch):
    # A API responde HTTP 400 quando os espaços do filtro chegam como +
    urls = []

    class Resposta:
        status_code = 200

        def json(self):
            return {"value": [{"Data": "2026-11-03"}]}

    monkeypatch.setattr(ingestao["requests"], "get", lambda url, **kw: urls.append(url) or Resposta())
    assert ingestao["buscar_pagina"]("http://exemplo/r", {"$filter": "Indicador eq 'IPCA'"}) == [{"Data": "2026-11-03"}]
    assert "Indicador%20eq%20%27IPCA%27" in urls[0] and "+" not in urls[0]


def test_prefixos_da_ingestao_batem_com_os_globs_do_pipeline():
    fontes = carregar_constante(INGESTAO, "FONTES")
    bronze = (TRANSFORMATIONS / "bronze" / "bronze_focus.py").read_text(encoding="utf-8")
    for prefixo in fontes:
        assert f'ler_focus("{prefixo}_*.json")' in bronze
    assert "/Volumes/projeto_hands_on/bronze/raw_focus" in bronze
    assert carregar_constante(TREINO, "ALPHA_CORRECAO") > 0
