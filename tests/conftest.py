"""Helpers to load code from Databricks notebooks without running them.

Notebooks depend on `spark` and `dbutils`, which only exist inside Databricks, so the
tests extract just the pure functions and constants from the source with `ast`.
"""

import ast
import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

RAIZ = Path(__file__).resolve().parent.parent
NOTEBOOKS = RAIZ / "databricks" / "notebooks"
TRANSFORMATIONS = RAIZ / "databricks" / "transformations"


def _arvore(caminho):
    return ast.parse(Path(caminho).read_text(encoding="utf-8"))


def carregar_funcoes(caminho, *nomes, **globais):
    """Executes only the given function definitions from a notebook and returns them in a namespace dict."""
    namespace = {
        "requests": requests,
        "json": json,
        "time": time,
        "datetime": datetime,
        "date": date,
        "timedelta": timedelta,
        **globais,
    }
    defs = [n for n in _arvore(caminho).body if isinstance(n, ast.FunctionDef) and n.name in nomes]
    encontrados = {d.name for d in defs}
    faltando = set(nomes) - encontrados
    assert not faltando, f"Funções não encontradas em {caminho}: {faltando}"
    # Notebooks may redefine a function in several cells; keep the last definition, as the notebook does
    exec(compile(ast.Module(body=defs, type_ignores=[]), str(caminho), "exec"), namespace)
    return namespace


def carregar_constante(caminho, nome):
    """Returns the literal value of the first top-level assignment `nome = ...` in the file."""
    for node in _arvore(caminho).body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == nome for t in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError(f"Constante {nome} não encontrada em {caminho}")
