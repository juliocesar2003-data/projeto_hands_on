"""Checks the configuration of gold.features_mensal (inflation targets, column names and comparisons)."""

import re

from conftest import TRANSFORMATIONS, carregar_constante

GOLD = TRANSFORMATIONS / "gold" / "gold_features.py"

METAS = carregar_constante(GOLD, "METAS_INFLACAO")
COLUNAS_Z = carregar_constante(GOLD, "COLUNAS_Z")
COMPARACOES_Z = carregar_constante(GOLD, "COMPARACOES_Z")


def test_metas_cobrem_todos_os_anos_da_tabela():
    inicio = int(carregar_constante(GOLD, "INICIO_FEATURES")[:4])
    anos = sorted(METAS)
    # Sem buracos: um ano sem meta deixaria ipca_12m_menos_meta nulo
    assert anos == list(range(anos[0], anos[-1] + 1))
    assert anos[0] <= inicio
    # Meta contínua de 3,00% +/- 1,5 p.p. desde 2025 (Resolução CMN nº 5.141)
    assert all(METAS[ano][:2] == (3.00, 1.50) for ano in anos if ano >= 2025)


def test_treino_comeca_no_inicio_da_tabela():
    assert carregar_constante(GOLD, "INICIO_TREINO") == carregar_constante(GOLD, "INICIO_FEATURES")
    assert carregar_constante(GOLD, "FIM_TREINO") < "2020-01-01"


def test_comparacoes_usam_colunas_padronizadas():
    padronizadas = {f"{c}_z" for c in COLUNAS_Z}
    for a, b in COMPARACOES_Z.values():
        assert {a, b} <= padronizadas and a != b


def test_nomes_usam_lag_em_vez_de_m():
    codigo = GOLD.read_text(encoding="utf-8")
    nomes = set(re.findall(r'alias\(f?"(\w+)"\)', codigo)) | set(re.findall(r'withColumn\("(\w+)"', codigo))
    # Ex.: ipca_m1 deve se chamar ipca_lag1 (janelas como acum_12m continuam com "m" de meses)
    assert not [n for n in nomes if re.search(r"_m\d+$", n)]
