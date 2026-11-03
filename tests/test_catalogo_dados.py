"""Checks that docs/catalogo_dados.md documents every table the project creates."""

import ast
import re

from conftest import NOTEBOOKS, RAIZ, TRANSFORMATIONS

CATALOGO = RAIZ / "docs" / "catalogo_dados.md"


def tabelas_do_pipeline():
    tabelas = set()
    for arquivo in (TRANSFORMATIONS / "bronze").glob("*.py"):
        # Bronze tables are created in the pipeline's default schema (bronze); append flows are not tables
        texto = re.sub(r"@dp\.append_flow\([^)]*\)", "", arquivo.read_text(encoding="utf-8"))
        tabelas |= {f"projeto_hands_on.bronze.{n}" for n in re.findall(r'name="(bronze_\w+)"', texto)}
    for camada in ("silver", "gold"):
        for arquivo in (TRANSFORMATIONS / camada).glob("*.py"):
            tabelas |= set(re.findall(rf'name="(projeto_hands_on\.{camada}\.\w+)"', arquivo.read_text(encoding="utf-8")))
    return tabelas


def tabelas_do_treino():
    """Tables written by gravar(df, "nome", ...) in the training notebook, in the default gold destination."""
    arvore = ast.parse((NOTEBOOKS / "treino_modelo_ipca.py").read_text(encoding="utf-8"))
    return {
        f"projeto_hands_on.gold.{no.args[1].value}"
        for no in ast.walk(arvore)
        if isinstance(no, ast.Call) and getattr(no.func, "id", None) == "gravar"
    }


def test_catalogo_documenta_todas_as_tabelas():
    secoes = set(re.findall(r"^#+ [\d.]+ `([\w.]+)`", CATALOGO.read_text(encoding="utf-8"), re.M))
    esperadas = tabelas_do_pipeline() | tabelas_do_treino()
    assert len(tabelas_do_treino()) == 4
    faltando = esperadas - secoes
    assert not faltando, f"Tabelas sem seção no catálogo: {sorted(faltando)}"
