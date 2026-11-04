"""Tests for the pure functions of the IPCA training notebook (no Spark or MLflow needed)."""

import warnings

import numpy as np
import pandas as pd
import pytest
from sklearn.linear_model import Ridge, RidgeCV
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.statespace.sarimax import SARIMAX

from conftest import NOTEBOOKS, TRANSFORMATIONS, carregar_constante, carregar_funcoes

NOTEBOOK = NOTEBOOKS / "treino_modelo_ipca.py"
HORIZONTES = list(range(1, 13))
ALVOS = {**{f"h{h}": f"alvo_ipca_h{h}" for h in HORIZONTES}, "acum_12m": "alvo_ipca_acum_12m"}

FEATURES_LINEAR = carregar_constante(NOTEBOOK, "FEATURES_LINEAR")
FEATURES_RF_EXTRAS = carregar_constante(NOTEBOOK, "FEATURES_RF_EXTRAS")
PERIODOS = carregar_constante(NOTEBOOK, "PERIODOS")


@pytest.fixture
def f():
    return carregar_funcoes(
        NOTEBOOK,
        "meses_a_frente", "compor", "preparar", "separar_por_ano", "medias_moveis", "previsao_referencia",
        "periodo_de", "ajustar_arima", "alinhar_exogenas", "previsoes_arima", "previsao_arima_alvo", "coeficientes_originais",
        "faixa_erro", "metricas",
        np=np, pd=pd, warnings=warnings, SARIMAX=SARIMAX, Ridge=Ridge, RidgeCV=RidgeCV,
        TimeSeriesSplit=TimeSeriesSplit, HORIZONTES=HORIZONTES, ALVOS=ALVOS, PERIODOS=PERIODOS,
    )


def _features(meses=72):
    """Small features_mensal-like frame: monthly base months from 2015, constant seasonal means."""
    base = pd.DataFrame({"mes_base": pd.date_range("2015-01-01", periods=meses, freq="MS")})
    for h in HORIZONTES:
        base[f"ipca_sazonal_h{h}"] = 0.5
        base[f"alvo_ipca_h{h}"] = 0.4
    base["alvo_ipca_acum_12m"] = 5.0
    base["ipca_lag1"], base["ipca_acum_12m"], base["media_movel_3m"] = 0.3, 4.0, 0.25
    return base


def test_meses_a_frente(f):
    assert f["meses_a_frente"]("h1") == 1
    assert f["meses_a_frente"]("h12") == 12
    assert f["meses_a_frente"]("acum_12m") == 12


def test_compor_acumula_em_juros_compostos(f):
    assert f["compor"]([1.0, 1.0]) == pytest.approx(2.01)
    assert f["compor"]([0.5] * 12) == pytest.approx((1.005**12 - 1) * 100)


def test_sazonal_acumulada_compoe_os_12_meses(f):
    dados = f["preparar"](_features(), "acum_12m")
    assert dados["ipca_sazonal"].iloc[0] == pytest.approx((1.005**12 - 1) * 100)
    assert (dados["y"] == 5.0).all()


@pytest.mark.parametrize("alvo", ["h1", "h6", "acum_12m"])
def test_origem_movel_nao_treina_com_alvo_do_ano_de_teste(f, alvo):
    dados = f["preparar"](_features(), alvo)
    treino, teste = f["separar_por_ano"](dados, alvo, 2018)
    meses = f["meses_a_frente"](alvo)

    assert (treino["mes_base"] + pd.DateOffset(months=meses) < "2018-01-01").all()
    assert (teste["mes_base"].dt.year == 2018).all() and len(teste) == 12
    # O treino usa tudo o que já era conhecido: meses base até 12/2017 menos o horizonte
    assert len(treino) == 36 - meses + 1 - 1


def test_ano_sem_alvo_conhecido_fica_sem_teste(f):
    dados = _features()
    dados.loc[dados["mes_base"].dt.year == 2020, "alvo_ipca_h1"] = None
    _, teste = f["separar_por_ano"](f["preparar"](dados, "h1"), "h1", 2020)
    assert teste.empty


def test_media_movel_usa_so_meses_ate_a_origem(f):
    ipca = pd.Series([1.0, 2.0, 3.0, 4.0], index=pd.date_range("2020-01-01", periods=4, freq="MS"))
    medias = f["medias_moveis"](ipca, [3])
    assert np.isnan(medias["media_movel_3m"].iloc[1])
    assert medias["media_movel_3m"].iloc[2] == pytest.approx(2.0)
    assert medias["media_movel_3m"].iloc[3] == pytest.approx(3.0)


def test_referencias(f):
    dados = f["preparar"](_features(), "h3")
    assert (f["previsao_referencia"](dados, "h3", "ultimo_mes") == 0.3).all()
    assert (f["previsao_referencia"](dados, "h3", "sazonal") == 0.5).all()
    assert (f["previsao_referencia"](dados, "h3", "media_movel_3m") == 0.25).all()

    acum = f["preparar"](_features(), "acum_12m")
    assert (f["previsao_referencia"](acum, "acum_12m", "ultimo_mes") == 4.0).all()
    assert f["previsao_referencia"](acum, "acum_12m", "media_movel_3m").iloc[0] == pytest.approx((1.0025**12 - 1) * 100)


def test_periodos(f):
    assert f["periodo_de"](pd.Timestamp("2005-01-01")) == "2005-2016"
    assert f["periodo_de"](pd.Timestamp("2018-06-01")) == "2017-2019"
    assert f["periodo_de"](pd.Timestamp("2021-12-01")) == "2020-2021"
    assert f["periodo_de"](pd.Timestamp("2027-03-01")) == "2022 em diante"


def _serie_sazonal():
    rng = np.random.default_rng(1)
    meses = pd.date_range("2010-01-01", periods=96, freq="MS")
    valores = 0.4 + 0.2 * np.sin(2 * np.pi * meses.month / 12) + rng.normal(0, 0.05, len(meses))
    return pd.Series(valores, index=meses).asfreq("MS")


def test_arima_nao_usa_dados_depois_da_origem(f):
    serie = _serie_sazonal()
    origem, fim_treino = pd.Timestamp("2015-06-01"), pd.Timestamp("2014-12-01")
    args = ((1, 0, 0), (1, 0, 0, 12), fim_treino, [origem])

    completa = f["previsoes_arima"](serie, *args)[origem]
    cortada = f["previsoes_arima"](serie[serie.index <= origem], *args)[origem]
    assert len(completa) == 12
    np.testing.assert_allclose(completa, cortada)


def test_arima_por_alvo(f):
    previsoes = {pd.Timestamp("2020-01-01"): np.full(12, 0.5)}
    origem = pd.Timestamp("2020-01-01")
    assert f["previsao_arima_alvo"](previsoes, origem, "h3") == 0.5
    assert f["previsao_arima_alvo"](previsoes, origem, "acum_12m") == pytest.approx((1.005**12 - 1) * 100)


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


def test_periodos_cobrem_os_testes_sem_sobreposicao():
    intervalos = sorted(PERIODOS.values())
    assert intervalos[0][0] == f"{carregar_constante(NOTEBOOK, 'PRIMEIRO_ANO_TESTE')}-01-01"
    for (_, fim), (inicio, _) in zip(intervalos, intervalos[1:]):
        assert pd.Timestamp(fim) + pd.DateOffset(months=1) == pd.Timestamp(inicio)


def test_features_do_modelo_existem_na_tabela_gold():
    gold = (TRANSFORMATIONS / "gold" / "gold_features.py").read_text(encoding="utf-8")
    assert not set(FEATURES_LINEAR) & set(FEATURES_RF_EXTRAS)
    # ipca_sazonal é montada no notebook a partir de ipca_sazonal_hN
    for feature in FEATURES_LINEAR + FEATURES_RF_EXTRAS:
        if feature != "ipca_sazonal":
            assert f'"{feature}"' in gold, f"{feature} não é coluna de gold.features_mensal"


def test_primeiro_teste_tem_historico_de_features():
    gold_inicio = carregar_constante(TRANSFORMATIONS / "gold" / "gold_features.py", "INICIO_FEATURES")
    # Pelo menos 3 anos de features antes do primeiro ano de teste
    assert int(gold_inicio[:4]) + 3 <= carregar_constante(NOTEBOOK, "PRIMEIRO_ANO_TESTE")


@pytest.fixture
def fx():
    return carregar_funcoes(
        NOTEBOOK, "exogenas_mensais", "alinhar_exogenas", "ajustar_arima", "previsoes_arima", "parametros_sarimax",
        np=np, pd=pd, warnings=warnings, SARIMAX=SARIMAX,
    )


def test_exogenas_seguem_as_definicoes_da_features_mensal(fx):
    meses = pd.date_range("2020-01-01", periods=13, freq="MS")
    ipca = pd.Series(0.5, index=meses)
    selic = pd.Series(np.linspace(10, 12.4, 13), index=meses)
    usd = pd.Series(np.linspace(5, 6.2, 13), index=meses)
    exog = fx["exogenas_mensais"](ipca, selic, usd).iloc[-1]

    assert exog["juro_real_12m"] == pytest.approx(12.4 - (1.005**12 - 1) * 100)
    assert exog["selic_var_12m_pp"] == pytest.approx(2.4)
    assert exog["usd_var_12m"] == pytest.approx((6.2 / 5 - 1) * 100)
    assert exog["usd_var_3m"] == pytest.approx((6.2 / 5.9 - 1) * 100)


def _serie_com_exogena():
    rng = np.random.default_rng(2)
    meses = pd.date_range("2008-01-01", periods=120, freq="MS")
    x = pd.Series(rng.normal(5, 2, len(meses)), index=meses)
    y = 0.4 + 0.03 * x.shift(1).fillna(5) + rng.normal(0, 0.05, len(meses))
    return pd.Series(y, index=meses).asfreq("MS"), pd.DataFrame({"x": x})


def test_sarimax_nao_usa_dados_depois_da_origem(fx):
    serie, exog = _serie_com_exogena()
    origem = pd.Timestamp("2015-06-01")
    args = ((1, 0, 0), (0, 0, 0, 12), pd.Timestamp("2014-12-01"), [origem])

    completa = fx["previsoes_arima"](serie, *args, exog=exog)[origem]
    cortada = fx["previsoes_arima"](serie[serie.index <= origem], *args, exog=exog[exog.index <= origem])[origem]
    np.testing.assert_allclose(completa, cortada)


def test_cenario_desloca_toda_a_previsao_pelo_coeficiente(fx):
    # Regressão com erros SARIMA: mudar a variável externa em delta (mantida nos 12 meses) muda cada mês
    # previsto em coeficiente x delta. É a conta que o simulador vai fazer em SQL.
    serie, exog = _serie_com_exogena()
    origem, fim = pd.Timestamp("2015-06-01"), pd.Timestamp("2014-12-01")
    cenario = exog.copy()
    cenario.loc[origem, "x"] += 2.0

    base = fx["previsoes_arima"](serie, (1, 0, 0), (0, 0, 0, 12), fim, [origem], exog=exog)[origem]
    alterada = fx["previsoes_arima"](serie, (1, 0, 0), (0, 0, 0, 12), fim, [origem], exog=cenario)[origem]
    defasadas = exog.shift(1).loc[serie.index[1:]]
    coef = fx["ajustar_arima"](serie[1:][serie.index[1:] <= fim], (1, 0, 0), (0, 0, 0, 12),
                               defasadas[defasadas.index <= fim]).params["x"]
    # O primeiro mês previsto usa a variável do mês de origem; os demais, o valor mantido
    np.testing.assert_allclose(alterada - base, np.full(12, 2.0 * coef), rtol=1e-6)


def test_parametros_sarimax_tem_intervalo(fx):
    serie, exog = _serie_com_exogena()
    ajuste = fx["ajustar_arima"](serie[1:], (1, 0, 0), (0, 0, 0, 12), exog.shift(1).iloc[1:])
    tabela = fx["parametros_sarimax"](ajuste)
    linha = tabela.set_index("parametro").loc["x"]
    assert linha["ic95_inferior"] < linha["coeficiente"] < linha["ic95_superior"]
    assert linha["coeficiente"] == pytest.approx(0.03, abs=0.01)


def test_alinhar_descarta_meses_sem_todas_as_exogenas(fx):
    meses = pd.date_range("2000-01-01", periods=6, freq="MS")
    serie = pd.Series(np.arange(6.0), index=meses)
    exog = pd.DataFrame({"a": [1.0, 2, 3, 4, 5, 6], "b": [np.nan, np.nan, 1, 1, 1, 1]}, index=meses)
    endog, defasadas = fx["alinhar_exogenas"](serie, exog)
    # O IPCA de t usa a exógena de t-1: o primeiro mês com "b" defasado é março
    assert list(endog.index) == list(meses[3:])
    assert defasadas.notna().all().all() and defasadas["a"].iloc[0] == 3.0


def test_modelo_oficial_e_a_media_corrigida_so_em_h1():
    assert carregar_constante(NOTEBOOK, "MODELO_OFICIAL") == "media_tres_corrigida"
    assert carregar_constante(NOTEBOOK, "ALVOS_CORRIGIDOS") == ["h1"]
    assert '"media_tres_corrigida"]' in NOTEBOOK.read_text(encoding="utf-8")
