import json
import os
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv() # carrega .env para os.environ

# Controle de protocolo HTTP
def requisitar_resiliente(url: str, params: dict) -> dict:
    headers = {"chave-api-dados": os.environ["API_KEY"]}
    url = os.environ["API_BASE_URL"] + url
    #print(f"Requisitando {url} com params {params} ...")
    tentativa = 0
    max_tentativas = 5
    while tentativa < max_tentativas:
        r = requests.get(url, params=params, headers=headers, timeout=30)
        if r.status_code == 200:
            return r.json()

        if r.status_code in (401, 403): # ← nunca retentar
            raise PermissionError(
                f"Falha de auth ({r.status_code}). Revise o token."
            )

        if r.status_code == 429:
            # O servidor diz quanto tempo esperar — use esse valor
            espera = int(r.headers.get("Retry-After", 2 ** tentativa))
            print(f"[429] Rate limit. Aguardando {espera}s .")
            time.sleep(espera)

        elif r.status_code >= 500: # estratégia de backoff exponencial para erros de servidor
            espera = 2 ** tentativa
            print(f"[{r.status_code}] Erro servidor. Retry em {espera}s .")
            time.sleep(espera)
        else:
            raise ValueError(
                f"Erro inesperado: {r.status_code} — {r.text[:200]}"
            )
        tentativa += 1
        print(f"Chamadas restantes nesta janela: {max_tentativas}")

    raise TimeoutError(
        f"Máximo de {max_tentativas} tentativas atingido para {url}"
    )

def salvar_com_rastreamento(dados: dict, destino: Path) -> None:
    # Garante a existência da pasta de saída antes de escrever os arquivos
    destino.parent.mkdir(parents=True, exist_ok=True)

    # Salva o dado bruto
    destino.write_text(json.dumps(dados, ensure_ascii=False, indent=2))

    # Salva metadado de schema ao lado
    if isinstance(dados, dict):
        campos = list(dados.keys())
        n_itens = None
    elif isinstance(dados, list):
        # Quando houver lista de objetos, agrega as chaves encontradas
        if dados and all(isinstance(item, dict) for item in dados):
            campos = sorted({chave for item in dados for chave in item.keys()})
        else:
            campos = []
        n_itens = len(dados)
    else:
        campos = []
        n_itens = None

    schema = {
        "tipo_dado": type(dados).__name__,
        "campos": campos,
        "n_campos": len(campos),
        "n_itens": n_itens,
        "coletado_em": datetime.now().isoformat(),
    }
    destino.with_suffix(".schema.json").write_text(
        json.dumps(schema, ensure_ascii=False))

# Salvar com idempotencia usando hash do conteúdo
import hashlib

PATH_INVENTARIO = Path("data/control/hashes.json")
def carregar_inventario() -> set:
    if PATH_INVENTARIO.exists():
        conteudo = PATH_INVENTARIO.read_text().strip()
        if conteudo:
            return set(json.loads(conteudo))
    return set() # 1a execução ou arquivo vazio

def salvar_idempotente(
    conteudo, destino: Path, inventario: set
) -> int:
    # Persiste apenas registros ainda não vistos
    registros = conteudo if isinstance(conteudo, list) else [conteudo]

    # Filtra por hash individual de cada registro
    novos = []
    for registro in registros:
        hash_r = hashlib.sha256(
            json.dumps(registro, sort_keys=True).encode()
        ).hexdigest()
        if hash_r not in inventario:
            novos.append((hash_r, registro))

    if not novos:
        return 0

    # Carrega registros já salvos em disco para acrescentar, não sobrescrever
    destino.parent.mkdir(parents=True, exist_ok=True)
    if destino.exists():
        existentes = json.loads(destino.read_text())
        if not isinstance(existentes, list):
            existentes = [existentes]
    else:
        existentes = []

    existentes.extend(registro for _, registro in novos)
    destino.write_text(json.dumps(existentes, ensure_ascii=False, indent=2))

    # Atualiza inventário em memória e em disco
    for hash_r, _ in novos:
        inventario.add(hash_r)
    PATH_INVENTARIO.parent.mkdir(parents=True, exist_ok=True)
    PATH_INVENTARIO.write_text(json.dumps(list(inventario)))

    return len(novos)

PATH_CONTROLE = Path("data/control/watermark.json")
def carregar_watermark() -> str:
    if PATH_CONTROLE.exists():
        return json.loads(PATH_CONTROLE.read_text())["atualizado_em"]
    # Data zero: na 1a execução faz Full Load implícito
    return "1970-01-01T00:00:00"

def salvar_watermark(timestamp: str) -> None:
    PATH_CONTROLE.parent.mkdir(parents=True, exist_ok=True)
    PATH_CONTROLE.write_text(
        json.dumps({
            "ultimo_registro": timestamp,
                "atualizado_em": datetime.now().astimezone().isoformat()
        })
    )

    # Latência do pipeline
    dados_watermark = json.loads(PATH_CONTROLE.read_text())
    ultimo = datetime.fromisoformat(dados_watermark["ultimo_registro"])
    coletado = datetime.fromisoformat(dados_watermark["atualizado_em"])

    latencia_min = (coletado - ultimo).total_seconds() / 60
    print(f"Latência do pipeline: {latencia_min:.1f} min")