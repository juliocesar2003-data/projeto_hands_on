"""Checks that the notebooks and the pipeline stay in sync and in Databricks format."""

import re
from fnmatch import fnmatch

import pytest
import yaml

from conftest import NOTEBOOKS, RAIZ, TRANSFORMATIONS, carregar_constante

DIARIA = NOTEBOOKS / "ingestao_diaria_bacen.py"
HISTORICA = NOTEBOOKS / "ingestao_bacen.py"

SERIES = carregar_constante(DIARIA, "SERIES")


def globs_do_pipeline():
    globs = []
    for arquivo in (TRANSFORMATIONS / "bronze").glob("*.py"):
        # O glob aparece direto no .option("pathGlobFilter", ...) ou na chamada ler_arquivos("...")
        globs += re.findall(r'(?:"pathGlobFilter",\s*|ler_arquivos\()"([^"]+)"', arquivo.read_text(encoding="utf-8"))
    return globs


def test_cada_serie_tem_um_glob_no_pipeline():
    globs = globs_do_pipeline()
    assert sorted(globs) == sorted(f"{serie}_*.json" for serie in SERIES)


@pytest.mark.parametrize("serie", list(SERIES))
def test_arquivos_gravados_sao_lidos_pelo_glob_da_serie(serie):
    globs = globs_do_pipeline()
    nomes = [
        f"{serie}_diario_20261024_210000.json",  # ingestão diária
        f"{serie}_2026_20261024.json",  # complemento 2026 da carga histórica
    ]
    for nome in nomes:
        # Cada arquivo precisa casar com exatamente um glob, senão vai para a tabela errada ou é duplicado
        assert [g for g in globs if fnmatch(nome, g)] == [f"{serie}_*.json"]


def test_carga_historica_cobre_as_mesmas_series():
    moedas = carregar_constante(HISTORICA, "moedas")
    indicadores = carregar_constante(HISTORICA, "indicadores")
    series_2026 = carregar_constante(HISTORICA, "series_2026")

    assert set(moedas) | set(indicadores) == set(SERIES)
    assert series_2026 == SERIES
    assert {nome: cfg["code"] for nome, cfg in indicadores.items()} == {n: SERIES[n] for n in indicadores}
    for nome, cfg in moedas.items():
        assert cfg["url"].endswith(f"bcdata.sgs.{SERIES[nome]}/dados")


@pytest.mark.parametrize("notebook", sorted(NOTEBOOKS.glob("*.py")), ids=lambda p: p.name)
def test_notebooks_mantem_formato_databricks(notebook):
    assert notebook.read_text(encoding="utf-8").startswith("# Databricks notebook source")


def test_bundle_aponta_para_arquivos_existentes():
    bundle = yaml.safe_load((RAIZ / "databricks.yml").read_text(encoding="utf-8"))
    jobs = bundle["resources"]["jobs"]
    pipeline = bundle["resources"]["pipelines"]["pipeline_bacen"]

    for job in jobs.values():
        for task in job["tasks"]:
            if "notebook_task" in task:
                assert (RAIZ / task["notebook_task"]["notebook_path"]).is_file()

    include = pipeline["libraries"][0]["glob"]["include"]
    assert include == "./databricks/transformations/**"
    assert {"catalog": "projeto_hands_on", "schema": "bronze", "serverless": True}.items() <= pipeline.items()

    diaria = jobs["ingestao_diaria_bacen"]
    assert diaria["schedule"]["timezone_id"] == "America/Sao_Paulo"
    assert [t["task_key"] for t in diaria["tasks"]] == ["ingestao_diaria", "atualiza_pipeline"]
    assert diaria["tasks"][1]["pipeline_task"]["pipeline_id"] == "${resources.pipelines.pipeline_bacen.id}"


def test_eda_le_somente_tabelas_silver_do_pipeline():
    eda = (NOTEBOOKS / "eda_bacen.py").read_text(encoding="utf-8")
    lidas = set(re.findall(r'ler_silver\("(\w+)"', eda))

    definidas = set()
    for arquivo in (TRANSFORMATIONS / "silver").glob("*.py"):
        definidas |= set(re.findall(r'name="projeto_hands_on\.silver\.(\w+)"', arquivo.read_text(encoding="utf-8")))

    assert lidas == {"moedas_conversao", "selic", "cdi", "ipca", "tr"}
    assert lidas <= definidas


def _materialized_views(camada):
    """Returns {table name: source of its definition block} for every materialized view of a layer."""
    blocos = {}
    for arquivo in (TRANSFORMATIONS / camada).glob("*.py"):
        texto = arquivo.read_text(encoding="utf-8")
        for bloco in texto.split("@dp.materialized_view(")[1:]:
            nome = re.search(rf'name="projeto_hands_on\.{camada}\.(\w+)"', bloco).group(1)
            blocos[nome] = bloco.split("\ndef ")[0]
    return blocos


def _materialized_views_silver():
    return _materialized_views("silver")


@pytest.mark.parametrize("camada", ["silver", "gold"])
def test_toda_tabela_tem_regras_de_qualidade(camada):
    blocos = _materialized_views(camada)
    assert blocos
    for nome, bloco in blocos.items():
        assert "@dp.expect" in bloco, f"{camada}.{nome} não tem regras de qualidade (@dp.expect...)"


def test_dq_resumo_cobre_todas_as_tabelas_silver():
    qualidade = (TRANSFORMATIONS / "silver" / "silver_qualidade.py").read_text(encoding="utf-8")
    bloco = qualidade.split("TABELAS = {")[1].split("}")[0]
    cobertas = set(re.findall(r'"(\w+)":', bloco))
    assert cobertas == set(_materialized_views_silver()) - {"dq_resumo"}


def test_toda_tabela_bronze_registra_origem_e_data_de_ingestao():
    moedas = (TRANSFORMATIONS / "bronze" / "bronze_moedas.py").read_text(encoding="utf-8")
    indicadores = (TRANSFORMATIONS / "bronze" / "bronze_indicadores.py").read_text(encoding="utf-8")

    # Moedas: as colunas estão no schema da streaming table e em cada um dos 5 fluxos
    assert "origem_ingestao STRING, data_ingestao TIMESTAMP, data_processamento TIMESTAMP" in moedas
    assert moedas.count("*COLUNAS_INGESTAO") == moedas.count("@dp.append_flow") == 5

    # Indicadores: toda tabela lê os arquivos pela função que adiciona as colunas
    assert indicadores.count("return ler_arquivos(") == indicadores.count("@dp.table(") == 4
    for coluna in ("origem_ingestao", "data_ingestao", "data_processamento"):
        assert f'.withColumn("{coluna}"' in indicadores
