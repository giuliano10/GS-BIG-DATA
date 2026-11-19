"""
GS 2026.2 · CitizenTech · Big Data
DAG de coleta: Caminho Livre (acessibilidade nas calçadas de São Paulo)

Fontes (uma tarefa por fonte, rodando em paralelo):
  1. SP156 (Portal de Dados Abertos da Prefeitura de SP, API CKAN):
     solicitações de cidadãos sobre calçadas.
  2. Open-Meteo: chuva e temperatura do dia em cada subprefeitura.
  3. Relatos do app Caminho Livre: barreiras detectadas pelo app (JSON aninhado,
     com dados pessoais FICTÍCIOS, gerados por um simulador da API do app).

Cada execução grava UM arquivo por fonte em chegada/<fonte>/, com a data no nome.
Depois, a tarefa conferir_chegada valida os arquivos e notificar registra o resultado.
Configurações e chaves ficam em Airflow Variables (nunca no código).
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import random
import re
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path

import pendulum
import requests
from airflow.sdk import DAG, Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

CHEGADA = Path(os.environ.get("GS_CHEGADA", "/opt/airflow/chegada"))
CONFIG = Path(__file__).parent / "config"

SP156_API_PADRAO = (
    "https://dados.prefeitura.sp.gov.br/api/3/action/package_show?id=dados-do-sp156"
)


# ---------------------------------------------------------------- utilidades
def _sem_acento(txt: str) -> str:
    txt = unicodedata.normalize("NFKD", str(txt))
    return "".join(c for c in txt if not unicodedata.combining(c))


def _nome_coluna(txt: str) -> str:
    txt = _sem_acento(txt).lower().strip()
    return re.sub(r"[^a-z0-9]+", "_", txt).strip("_") or "coluna"


def _subprefeituras() -> list[dict]:
    with open(CONFIG / "subprefeituras.csv", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _var(chave: str, padrao: str | None = None) -> str | None:
    """Lê uma Airflow Variable; usa o padrão só para itens que não são segredo."""
    try:
        return Variable.get(chave)
    except Exception:  # Variable inexistente
        return padrao


def _gravar_atomico(destino: Path, conteudo: str) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    tmp = destino.with_suffix(destino.suffix + ".tmp")
    tmp.write_text(conteudo, encoding="utf-8")
    tmp.replace(destino)


# ---------------------------------------------------------- fonte 1: SP156
def _get_portal(url: str, **kwargs) -> requests.Response:
    """GET no portal da Prefeitura.

    O servidor dados.prefeitura.sp.gov.br não envia o certificado intermediário (cadeia SSL incompleta),
    então a verificação padrão falha. Nesse caso repetimos sem verificar o certificado: é um portal
    público de dados abertos e nenhuma credencial é enviada. O aviso fica registrado no log.
    """
    kwargs.setdefault("headers", {"Accept-Encoding": "identity", "User-Agent": "gs-caminho-livre/1.0"})
    try:
        return requests.get(url, **kwargs)
    except requests.exceptions.SSLError:
        print("AVISO: cadeia SSL incompleta no portal da Prefeitura; baixando sem verificar o certificado.")
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        return requests.get(url, verify=False, **kwargs)


def _escolher_recurso_sp156() -> tuple[str, str]:
    """Retorna (url, nome) do CSV trimestral mais recente de solicitações ("Dados do SP156 - ...")."""
    manual = _var("sp156_csv_url")
    if manual:
        return manual, manual.rsplit("/", 1)[-1]

    r = _get_portal(_var("sp156_api_url", SP156_API_PADRAO), timeout=60)
    r.raise_for_status()
    # o conjunto também tem "Avaliação dos canais", dicionários e PDFs: só interessam as solicitações.
    # A API lista os trimestres do mais recente para o mais antigo.
    recursos = [
        rec for rec in r.json()["result"]["resources"]
        if (rec.get("name") or "").strip().lower().startswith("dados do sp156")
        and (rec.get("url") or "").lower().endswith(".csv")
    ]
    if not recursos:
        raise RuntimeError("Nenhum CSV 'Dados do SP156' encontrado no conjunto.")
    return recursos[0]["url"], recursos[0]["name"].strip()


def _baixar_com_cache(url: str) -> Path:
    """Baixa o CSV do SP156 (cerca de 100 MB) uma vez só e reaproveita nas próximas execuções.

    O servidor da Prefeitura derruba conexões longas, então o download é retomado de onde parou
    (cabeçalho Range) em até 30 tentativas.
    """
    cache = CHEGADA / "_cache" / "sp156"
    cache.mkdir(parents=True, exist_ok=True)
    manual = cache / "manual.csv"  # alternativa: baixar no navegador e salvar com esse nome
    if manual.exists():
        return manual
    destino = cache / (url.rsplit("/", 1)[-1] or "sp156.csv")
    if destino.exists() and destino.stat().st_size > 0:
        return destino

    parcial = destino.with_suffix(".part")
    total = None
    for tentativa in range(1, 31):
        ja_baixado = parcial.stat().st_size if parcial.exists() else 0
        if total and ja_baixado >= total:
            break
        cab = {"Accept-Encoding": "identity", "User-Agent": "gs-caminho-livre/1.0"}
        if ja_baixado:
            cab["Range"] = f"bytes={ja_baixado}-"
        try:
            with _get_portal(url, headers=cab, stream=True, timeout=(30, 120)) as r:
                r.raise_for_status()
                if r.status_code == 200:  # servidor ignorou o Range: recomeça do zero
                    ja_baixado = 0
                    total = int(r.headers.get("Content-Length") or 0) or None
                elif r.status_code == 206:
                    faixa = r.headers.get("Content-Range", "")
                    total = int(faixa.rsplit("/", 1)[-1]) if "/" in faixa else total
                with open(parcial, "ab" if ja_baixado else "wb") as f:
                    for bloco in r.iter_content(chunk_size=1 << 20):
                        f.write(bloco)
            if total is None or parcial.stat().st_size >= total:
                break
        except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as erro:
            print(f"Tentativa {tentativa}: conexão caiu com {parcial.stat().st_size if parcial.exists() else 0} bytes ({erro}); retomando...")
    else:
        raise RuntimeError("Não foi possível concluir o download do SP156 em 30 tentativas.")
    parcial.replace(destino)
    print(f"Download concluído: {destino.name} ({destino.stat().st_size / 1e6:.1f} MB)")
    return destino


def coletar_sp156(ds: str, **_):
    url, nome_recurso = _escolher_recurso_sp156()
    arquivo = _baixar_com_cache(url)

    # detecta a codificação pelo início do arquivo e lê em streaming (o arquivo é grande)
    with open(arquivo, "rb") as f:
        amostra = f.read(1 << 20)
    try:
        amostra.decode("utf-8")
        codificacao = "utf-8-sig"
    except UnicodeDecodeError:
        codificacao = "latin-1"
    primeira = amostra.split(b"\n", 1)[0].decode(codificacao, errors="ignore")
    sep = ";" if primeira.count(";") > primeira.count(",") else ","

    saida = io.StringIO()
    escritor = csv.writer(saida)
    total = 0
    with open(arquivo, encoding=codificacao, errors="replace", newline="") as f:
        leitor = csv.reader(f, delimiter=sep)
        cabecalho = [_nome_coluna(c) for c in next(leitor)]
        i_tema = cabecalho.index("tema") if "tema" in cabecalho else None
        i_serv = cabecalho.index("servico") if "servico" in cabecalho else None
        escritor.writerow(cabecalho + ["data_coleta", "recurso_origem"])
        for linha in leitor:
            if len(linha) != len(cabecalho):
                continue
            tema = _sem_acento(linha[i_tema]).lower() if i_tema is not None else ""
            servico = _sem_acento(linha[i_serv]).lower() if i_serv is not None else " ".join(linha).lower()
            # só a condição da calçada (manutenção, obstáculos, acessibilidade, fiação, construção);
            # ficam de fora poda de árvore, lavagem de calçadão e acesso de veículos a calçadões
            if "calcad" in servico and tema.startswith("rua e bairro"):
                escritor.writerow(linha + [ds, nome_recurso])
                total += 1

    if total == 0:
        raise RuntimeError("Nenhuma solicitação de calçada encontrada no recurso SP156.")
    _gravar_atomico(CHEGADA / "sp156" / f"sp156_{ds}.csv", saida.getvalue())
    print(f"SP156: {total} solicitações de calçada gravadas (recurso: {nome_recurso})")


# -------------------------------------------------------- fonte 2: Open-Meteo
def coletar_clima(ds: str, **_):
    subs = _subprefeituras()
    dia = date.fromisoformat(ds)
    # arquivo histórico tem atraso de alguns dias; para datas recentes usamos a API de previsão
    if (date.today() - dia).days > 7:
        url = _var("openmeteo_archive_url", "https://archive-api.open-meteo.com/v1/archive")
    else:
        url = _var("openmeteo_forecast_url", "https://api.open-meteo.com/v1/forecast")

    r = requests.get(
        url,
        params={
            "latitude": ",".join(s["latitude"] for s in subs),
            "longitude": ",".join(s["longitude"] for s in subs),
            "start_date": ds,
            "end_date": ds,
            "daily": "precipitation_sum,temperature_2m_max,temperature_2m_min",
            "timezone": "America/Sao_Paulo",
        },
        timeout=60,
    )
    r.raise_for_status()
    dados = r.json()
    if isinstance(dados, dict):  # uma localização só
        dados = [dados]

    linhas = []
    for sub, resp in zip(subs, dados):
        resp["subprefeitura_codigo"] = sub["codigo"]
        resp["subprefeitura"] = sub["subprefeitura"]
        resp["data_coleta"] = ds
        linhas.append(json.dumps(resp, ensure_ascii=False))
    _gravar_atomico(CHEGADA / "clima" / f"clima_{ds}.json", "\n".join(linhas) + "\n")
    print(f"Open-Meteo: {len(linhas)} subprefeituras gravadas")


# ------------------------------------------------- fonte 3: relatos do app
NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fabio", "Gabi", "Heitor", "Iris", "João",
         "Karen", "Lucas", "Marina", "Nicolas", "Olivia", "Paulo", "Rafa", "Sofia", "Tiago", "Vera"]
SOBRENOMES = ["Teste", "Exemplo", "Ficticio", "Simulado", "Demo"]
TIPOS = ["buraco", "desnivel", "obstaculo_fixo", "lixo_entulho", "veiculo_na_calcada",
         "rampa_ausente", "piso_tatil_danificado", "calcada_estreita"]


def coletar_relatos(ds: str, **_):
    """Simula a API do app Caminho Livre (o app real ainda não tem backend público).

    O token fica na Variable relatos_api_token e é obrigatório, como seria numa API real.
    Os dados pessoais são FICTÍCIOS (domínio exemplo.com) e servem para demonstrar a LGPD na Ouro.
    Cerca de 10% dos relatos vêm com problemas de propósito, para testar as regras de qualidade.
    """
    token = _var("relatos_api_token")
    if not token:
        raise RuntimeError("Defina a Variable 'relatos_api_token' no Airflow (Admin > Variables).")

    subs = _subprefeituras()
    rnd = random.Random(f"{ds}-{token}")
    usuarios = [
        {
            "nome": f"{n} {s}",
            "email": f"{n.lower()}.{s.lower()}{i}@exemplo.com",
            "telefone": f"(11) 9{rnd.randint(1000, 9999)}-{rnd.randint(1000, 9999)}",
        }
        for i, (n, s) in enumerate((n, s) for n in NOMES for s in SOBRENOMES[:2])
    ]
    base = datetime.fromisoformat(ds)
    relatos = []
    for i in range(rnd.randint(40, 70)):
        sub = rnd.choice(subs)
        relato = {
            "id_relato": f"CL-{ds.replace('-', '')}-{i:04d}",
            "enviado_em": (base + timedelta(minutes=rnd.randint(360, 1380))).isoformat(),
            "usuario": rnd.choice(usuarios),
            "localizacao": {
                "latitude": round(float(sub["latitude"]) + rnd.uniform(-0.015, 0.015), 6),
                "longitude": round(float(sub["longitude"]) + rnd.uniform(-0.015, 0.015), 6),
                "subprefeitura_codigo": sub["codigo"],
            },
            "deteccao": {
                "tipo": rnd.choice(TIPOS),
                "confianca": round(rnd.uniform(0.55, 0.99), 3),
                "gravidade": rnd.choice(["baixa", "media", "alta"]),
                "passagem_possivel": rnd.random() > 0.35,
            },
            "versao_app": rnd.choice(["1.2.0", "1.3.0"]),
        }
        defeito = rnd.random()
        if defeito < 0.03:
            relato["localizacao"]["latitude"] = None             # sem localização
        elif defeito < 0.06:
            relato["localizacao"]["latitude"] = -22.90           # fora de São Paulo
        elif defeito < 0.08:
            relato["deteccao"]["confianca"] = 0.21              # detecção pouco confiável
        elif defeito < 0.10:
            relato["deteccao"]["tipo"] = "desconhecido"          # tipo inválido
        relatos.append(relato)

    # reenvio: o app reenvia alguns relatos do dia anterior com a gravidade atualizada
    ontem = (base - timedelta(days=1)).strftime("%Y%m%d")
    for i in range(3):
        relatos.append({**relatos[i], "id_relato": f"CL-{ontem}-{i:04d}",
                        "enviado_em": (base + timedelta(hours=23)).isoformat(),
                        "deteccao": {**relatos[i]["deteccao"], "gravidade": "alta"}})

    conteudo = "\n".join(json.dumps(r, ensure_ascii=False) for r in relatos) + "\n"
    _gravar_atomico(CHEGADA / "relatos" / f"relatos_{ds}.json", conteudo)
    print(f"Relatos do app: {len(relatos)} relatos gravados")


# --------------------------------------------- conferência e notificação
ARQUIVOS = {"sp156": "sp156_{ds}.csv", "clima": "clima_{ds}.json", "relatos": "relatos_{ds}.json"}


def conferir_chegada(ds: str, **_):
    faltando, resumo = [], {}
    for fonte, modelo in ARQUIVOS.items():
        arq = CHEGADA / fonte / modelo.format(ds=ds)
        if not arq.exists() or arq.stat().st_size == 0:
            faltando.append(str(arq))
            continue
        with open(arq, encoding="utf-8") as f:
            linhas = sum(1 for _ in f)
        resumo[fonte] = {"arquivo": arq.name, "bytes": arq.stat().st_size,
                         "registros": linhas - (1 if fonte == "sp156" else 0)}
    if faltando:
        raise FileNotFoundError(f"Arquivos não chegaram: {faltando}")
    print(json.dumps(resumo, indent=2, ensure_ascii=False))
    return resumo


def _registrar(mensagem: str) -> None:
    pasta = CHEGADA / "_notificacoes"
    pasta.mkdir(parents=True, exist_ok=True)
    with open(pasta / "notificacoes.log", "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat(timespec='seconds')} | {mensagem}\n")
    webhook = _var("webhook_notificacao")  # opcional: URL de webhook do Teams/Discord/Slack
    if webhook:
        try:
            requests.post(webhook, json={"text": mensagem, "content": mensagem}, timeout=15)
        except Exception as erro:  # notificação nunca derruba o pipeline
            print(f"Falha ao enviar webhook: {erro}")


def notificar(ds: str, ti, **_):
    resumo = ti.xcom_pull(task_ids="conferir_chegada") or {}
    partes = ", ".join(f"{k}={v['registros']}" for k, v in resumo.items())
    _registrar(f"[OK] gs_coleta_calcadas {ds}: {partes}")


def avisar_falha(context):
    ti = context["ti"]
    _registrar(f"[FALHA] {ti.dag_id}.{ti.task_id} em {context.get('ds')}: {context.get('exception')}")


# ------------------------------------------------------------------- DAG
with DAG(
    dag_id="gs_coleta_calcadas",
    description="Coleta diária: SP156 (calçadas), Open-Meteo e relatos do app Caminho Livre",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 10, tz="America/Sao_Paulo"),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1),
                  "on_failure_callback": avisar_falha},
    tags=["gs", "citizentech", "calcadas"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")
    sp156 = PythonOperator(task_id="coletar_sp156", python_callable=coletar_sp156,
                           execution_timeout=timedelta(minutes=30))
    clima = PythonOperator(task_id="coletar_clima", python_callable=coletar_clima)
    relatos = PythonOperator(task_id="coletar_relatos_app", python_callable=coletar_relatos)
    conferir = PythonOperator(task_id="conferir_chegada", python_callable=conferir_chegada)
    aviso = PythonOperator(task_id="notificar", python_callable=notificar)

    inicio >> [sp156, clima, relatos] >> conferir >> aviso
