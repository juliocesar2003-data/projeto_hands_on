"""Tests for the pure functions of the IPCA training notebook (no Spark or MLflow needed)."""

import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from conftest import NOTEBOOKS, TRANSFORMATIONS, carregar_constante, carregar_funcoes

NOTEBOOK = NOTEBOOKS / "treino_modelo_ipca.py"
HORIZONTES = list(range(1, 13))
ALVOS = {**{f"h{h}": f"alvo_ipca_h{h}" for h in HORIZONTES}, "acum_12m": "alvo_ipca_acum_12m"}

FEATURES_LINEAR = carregar_constante(NOTEBOOK, "FEATURES_LINEAR")
FEATURES_RF_EXTRAS = carregar_constante(NOTEBOOK, "FEATURES_RF_EXTRAS")


@pytest.fixture
def f():
    return carregar_funcoes(
        NOTEBOOK,
        "meses_a_frente", "preparar", "separar_treino_teste", "previsao_referencia",
        "coeficientes_originais", "faixa_erro", "metricas",
        np=np, pd=pd, HORIZONTES=HORIZONTES, ALVOS=ALVOS,
    )


def _features(meses=60):
    """Small features_mensal-like frame: monthly base months from 2015, constant seasonal means."""
    base = pd.DataFrame({"mes_base": pd.date_range("2015-01-01", periods=meses, freq="MS")})
    for h in HORIZONTES:
        base[f"ipca_sazonal_h{h}"] = 0.5
        base[f"alvo_ipca_h{h}"] = 0.4
    base["alvo_ipca_acum_12m"] = 5.0
    base["ipca_lag1"], base["ipca_acum_12m"] = 0.3, 4.0
    return base


def test_meses_a_frente(f):
    assert f["meses_a_frente"]("h1") == 1
    assert f["meses_a_frente"]("h12") == 12
    assert f["meses_a_frente"]("acum_12m") == 12


def test_sazonal_acumulada_compoe_os_12_meses(f):
    dados = f["preparar"](_features(), "acum_12m")
    assert dados["ipca_sazonal"].iloc[0] == pytest.approx((1.005**12 - 1) * 100)
    assert (dados["y"] == 5.0).all()


@pytest.mark.parametrize("alvo", ["h1", "h6", "acum_12m"])
def test_treino_nao_tem_alvo_no_periodo_de_teste(f, alvo):
    dados = f["preparar"](_features(), alvo)
    treino, teste = f["separar_treino_teste"](dados, alvo, "2017-12-01")
    meses = f["meses_a_frente"](alvo)

    assert (treino["mes_base"] + pd.DateOffset(months=meses) <= "2017-12-01").all()
    assert (teste["mes_base"] > "2017-12-01").all()
    # Meses base cujo alvo cai no teste ficam fora dos dois conjuntos
    assert len(dados) - len(treino) - len(teste) == meses


def test_sem_alvo_fica_fora_do_treino_e_do_teste(f):
    dados = _features()
    dados.loc[dados.index[-3:], "alvo_ipca_h1"] = None
    treino, teste = f["separar_treino_teste"](f["preparar"](dados, "h1"), "h1", "2017-12-01")
    assert treino["y"].notna().all() and teste["y"].notna().all()
    assert len(teste) == 24 - 3  # 2018 e 2019, menos os 3 últimos meses sem alvo


def test_referencias(f):
    dados = f["preparar"](_features(), "h3")
    assert (f["previsao_referencia"](dados, "h3", "ultimo_mes") == 0.3).all()
    assert (f["previsao_referencia"](dados, "h3", "sazonal") == 0.5).all()
    assert (f["previsao_referencia"](f["preparar"](_features(), "acum_12m"), "acum_12m", "ultimo_mes") == 4.0).all()


def test_coeficientes_originais_reproduzem_a_previsao_do_modelo(f):
    rng = np.random.default_rng(0)
    x = pd.DataFrame(rng.normal([0.4, 10, 5], [0.3, 4, 8], size=(200, 3)), columns=["a", "b", "c"])
    y = 0.2 + 0.8 * x["a"] - 0.01 * x["b"] + 0.02 * x["c"] + rng.normal(0, 0.05, 200)
    modelo = make_pipeline(StandardScaler(), Ridge(alpha=5.0)).fit(x, y)

    intercepto, coeficientes = f["coeficientes_originais"](modelo, ["a", "b", "c"])
    manual = intercepto + sum(coeficientes[c] * x[c] for c in ["a", "b", "c"])
    # É exatamente a conta que o simulador em SQL vai fazer
    np.testing.assert_allclose(manual, modelo.predict(x), atol=1e-9)


def test_faixa_e_metricas(f):
    reais, previstos = [1.0, 2.0, 3.0, 4.0], [1.5, 2.0, 2.5, 4.0]
    inferior, superior = f["faixa_erro"](reais, previstos, (0.0, 1.0))
    assert (inferior, superior) == (-0.5, 0.5)
    m = f["metricas"](reais, previstos)
    assert m["mae"] == pytest.approx(0.25)
    assert m["rmse"] == pytest.approx(np.sqrt(0.125))
    assert m["n_teste"] == 4


def test_features_do_modelo_existem_na_tabela_gold():
    gold = (TRANSFORMATIONS / "gold" / "gold_features.py").read_text(encoding="utf-8")
    assert not set(FEATURES_LINEAR) & set(FEATURES_RF_EXTRAS)
    # ipca_sazonal é montada no notebook a partir de ipca_sazonal_hN
    for feature in FEATURES_LINEAR + FEATURES_RF_EXTRAS:
        if feature != "ipca_sazonal":
            assert f'"{feature}"' in gold, f"{feature} não é coluna de gold.features_mensal"
