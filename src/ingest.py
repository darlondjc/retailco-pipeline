import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import requests


# Controle de protocolo HTTP
def requisitar_resiliente(url: str, params: dict) -> dict:
    
    #validações parametros obrigatórios
    if "pagina" not in params or not isinstance(params["pagina"], int) or params["pagina"] < 1:
        raise ValueError("Parâmetro 'pagina' deve ser informado e deve ser um inteiro positivo")
    
    # chave-api-dados é como a API do Portal da Transparência espera o token de acesso, mas isso pode variar conforme a API alvo
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
                f"Falha de auth ({r.status_code}). Revise o token ou o nome da chave especificada na API."
            )

        if r.status_code == 429:
            # O servidor diz quanto tempo esperar — use esse valor
            espera = int(r.headers.get("Retry-After", 2 ** tentativa + random.randint(0, 1)))
            print(f"[429] Rate limit. Aguardando {espera}s .")
            time.sleep(espera)

        elif r.status_code >= 500: # estratégia de backoff exponencial com jitter aleatorio para erros de servidor
            espera = 2 ** tentativa + random.randint(0, 1)
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

def salvar_json_schema(dados: dict, destino: Path, coletado_em: str | None = None) -> str:
    # Garante a existência da pasta de saída para salvar o schema
    destino.parent.mkdir(parents=True, exist_ok=True)

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

    if coletado_em is None:
        coletado_em = datetime.now().astimezone().isoformat()

    schema = {
        "tipo_dado": type(dados).__name__,
        "campos": campos,
        "n_campos": len(campos),
        "n_itens": n_itens,
        "coletado_em": coletado_em,
    }
    destino.with_suffix(".schema.json").write_text(
        json.dumps(schema, ensure_ascii=False))
    return coletado_em

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
    # Persiste registros sem duplicar e reconstrói o raw se ele tiver sido apagado
    registros = conteudo if isinstance(conteudo, list) else [conteudo]

    destino.parent.mkdir(parents=True, exist_ok=True)
    if destino.exists():
        existentes = json.loads(destino.read_text())
        if not isinstance(existentes, list):
            existentes = [existentes]
    else:
        existentes = []

    hashes_persistidos = set()
    registros_persistidos = []

    for registro in existentes:
        hash_r = hashlib.sha256(
            json.dumps(registro, sort_keys=True).encode()
        ).hexdigest()
        if hash_r not in hashes_persistidos:
            hashes_persistidos.add(hash_r)
            registros_persistidos.append(registro)

    #cria aquivo de hash do inventario se não existir
    PATH_INVENTARIO.parent.mkdir(parents=True, exist_ok=True)
    novos = 0
    for registro in registros:
        hash_r = hashlib.sha256(
            json.dumps(registro, sort_keys=True).encode()
        ).hexdigest()

        if hash_r not in inventario:
            inventario.add(hash_r)
            novos += 1

        if hash_r not in hashes_persistidos:
            hashes_persistidos.add(hash_r)
            registros_persistidos.append(registro)

    destino.write_text(json.dumps(registros_persistidos, ensure_ascii=False, indent=2))

    # Atualiza inventário em memória e em disco
    PATH_INVENTARIO.write_text(json.dumps(list(inventario)))

    return novos

PATH_CONTROLE = Path("data/control/watermark.json")

def _normalizar_datetime(valor: str) -> datetime:
    dt = datetime.fromisoformat(valor)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def carregar_watermark() -> str:
    if PATH_CONTROLE.exists():
        return json.loads(PATH_CONTROLE.read_text())["atualizado_em"]
    # Data zero: na 1a execução faz Full Load implícito
    return "1970-01-01T00:00:00+00:00"


def salvar_watermark(timestamp: str, atualizado_em: str | None = None) -> None:
    ultimo = _normalizar_datetime(timestamp)

    if atualizado_em is None:
        atualizado_em = datetime.now(timezone.utc).isoformat()

    coletado = _normalizar_datetime(atualizado_em)

    PATH_CONTROLE.parent.mkdir(parents=True, exist_ok=True)
    PATH_CONTROLE.write_text(
        json.dumps({
            "ultimo_registro": ultimo.isoformat(),
            "atualizado_em": coletado.isoformat()
        })
    )

    latencia_min = (coletado - ultimo).total_seconds() / 60
    print(f"Latência do pipeline: {latencia_min:.1f} min")