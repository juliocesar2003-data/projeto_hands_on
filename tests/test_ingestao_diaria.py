from datetime import date, timedelta

import pytest
import requests

from conftest import NOTEBOOKS, carregar_funcoes

NOTEBOOK = NOTEBOOKS / "ingestao_diaria_bacen.py"


@pytest.fixture
def funcoes():
    return carregar_funcoes(NOTEBOOK, "gerar_janelas_10_anos", "buscar_janela", "filtrar_novos", HEADERS={})


# --- gerar_janelas_10_anos -------------------------------------------------------------


def test_janela_de_um_dia(funcoes):
    dia = date(2026, 10, 24)
    assert funcoes["gerar_janelas_10_anos"](dia, dia) == [(dia, dia)]


def test_intervalo_curto_gera_uma_janela(funcoes):
    assert funcoes["gerar_janelas_10_anos"](date(2026, 10, 20), date(2026, 10, 24)) == [
        (date(2026, 10, 20), date(2026, 10, 24))
    ]


def test_intervalo_longo_e_dividido_sem_buracos_nem_sobreposicao(funcoes):
    inicio, fim = date(1980, 1, 1), date(2026, 10, 24)
    janelas = funcoes["gerar_janelas_10_anos"](inicio, fim)

    assert janelas[0][0] == inicio
    assert janelas[-1][1] == fim
    for (_, fim_anterior), (inicio_seguinte, _) in zip(janelas, janelas[1:]):
        assert inicio_seguinte == fim_anterior + timedelta(days=1)
    for janela_inicio, janela_fim in janelas:
        assert janela_inicio <= janela_fim
        assert janela_fim <= janela_inicio.replace(year=janela_inicio.year + 10)


def test_inicio_em_29_de_fevereiro(funcoes):
    janelas = funcoes["gerar_janelas_10_anos"](date(2016, 2, 29), date(2030, 1, 1))
    assert janelas[0] == (date(2016, 2, 29), date(2026, 2, 28))


def test_inicio_depois_do_fim_nao_gera_janelas(funcoes):
    assert funcoes["gerar_janelas_10_anos"](date(2026, 10, 25), date(2026, 10, 24)) == []


# --- filtrar_novos ---------------------------------------------------------------------


def test_descarta_registros_ja_gravados(funcoes):
    # A API devolve o mês anterior a dataInicial em séries mensais (ex.: IPCA)
    registros = [
        {"data": "01/07/2026", "valor": "0.26"},
        {"data": "01/08/2026", "valor": "-0.32"},
        {"data": "01/09/2026", "valor": "0.82"},
    ]
    novos = funcoes["filtrar_novos"](registros, date(2026, 7, 1), date(2026, 10, 24))
    assert [r["data"] for r in novos] == ["01/08/2026", "01/09/2026"]


def test_descarta_datas_futuras(funcoes):
    # A Selic meta é publicada com datas futuras (até a próxima reunião do Copom)
    registros = [{"data": f"{d:02d}/10/2026", "valor": "13.75"} for d in range(23, 28)]
    novos = funcoes["filtrar_novos"](registros, date(2026, 10, 22), date(2026, 10, 24))
    assert [r["data"] for r in novos] == ["23/10/2026", "24/10/2026"]


def test_descarta_datas_duplicadas(funcoes):
    registros = [{"data": "23/10/2026", "valor": "5.0377"}, {"data": "23/10/2026", "valor": "5.0377"}]
    novos = funcoes["filtrar_novos"](registros, date(2026, 10, 22), date(2026, 10, 24))
    assert novos == [{"data": "23/10/2026", "valor": "5.0377"}]


def test_segunda_execucao_no_mesmo_dia_nao_repete(funcoes):
    registros = [{"data": "23/10/2026", "valor": "5.0377"}]
    primeira = funcoes["filtrar_novos"](registros, date(2026, 10, 22), date(2026, 10, 24))
    # Depois da primeira execução, a última data gravada passa a ser 23/10
    segunda = funcoes["filtrar_novos"](registros, date(2026, 10, 23), date(2026, 10, 24))
    assert len(primeira) == 1
    assert segunda == []


# --- buscar_janela ---------------------------------------------------------------------


class RespostaFalsa:
    def __init__(self, status_code, corpo):
        self.status_code = status_code
        self._corpo = corpo

    def json(self):
        if isinstance(self._corpo, Exception):
            raise self._corpo
        return self._corpo


@pytest.fixture
def api(funcoes, monkeypatch):
    """Replaces requests.get with a queue of fake answers and disables the waits between retries."""
    respostas = []
    chamadas = []

    def get_falso(url, params=None, headers=None, timeout=None):
        chamadas.append(params)
        resposta = respostas.pop(0)
        if isinstance(resposta, Exception):
            raise resposta
        return resposta

    monkeypatch.setattr(requests, "get", get_falso)
    monkeypatch.setattr(funcoes["time"], "sleep", lambda _: None)
    return respostas, chamadas


DIA = date(2026, 10, 23)
REGISTRO = [{"data": "23/10/2026", "valor": "5.0377"}]
SEM_DADOS = {"erro": {"statusCode": 404, "detail": "Value(s) not found"}}


def test_resposta_200_com_lista(funcoes, api):
    respostas, chamadas = api
    respostas.append(RespostaFalsa(200, REGISTRO))
    assert funcoes["buscar_janela"](1, DIA, DIA) == REGISTRO
    assert chamadas[0] == {"formato": "json", "dataInicial": "23/10/2026", "dataFinal": "23/10/2026"}


def test_resposta_404_e_sem_dados(funcoes, api):
    respostas, _ = api
    respostas.append(RespostaFalsa(404, SEM_DADOS))
    assert funcoes["buscar_janela"](1, DIA, DIA) == []


def test_resposta_200_com_erro_404_no_corpo_e_sem_dados(funcoes, api):
    respostas, _ = api
    respostas.append(RespostaFalsa(200, SEM_DADOS))
    assert funcoes["buscar_janela"](12, DIA, DIA) == []


def test_tenta_de_novo_apos_erro_de_conexao(funcoes, api):
    respostas, chamadas = api
    respostas.extend([requests.exceptions.ConnectionError(), RespostaFalsa(200, REGISTRO)])
    assert funcoes["buscar_janela"](1, DIA, DIA) == REGISTRO
    assert len(chamadas) == 2


def test_tenta_de_novo_apos_json_invalido(funcoes, api):
    respostas, _ = api
    respostas.extend([RespostaFalsa(200, ValueError("html")), RespostaFalsa(200, REGISTRO)])
    assert funcoes["buscar_janela"](1, DIA, DIA) == REGISTRO


def test_retorna_none_apos_tres_falhas(funcoes, api):
    respostas, chamadas = api
    respostas.extend([RespostaFalsa(500, None), RespostaFalsa(503, None), RespostaFalsa(200, {"outro": 1})])
    assert funcoes["buscar_janela"](1, DIA, DIA) is None
    assert len(chamadas) == 3
