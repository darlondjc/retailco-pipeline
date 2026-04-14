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
    
    #validações parametros obrigatórios
    if not url.startswith("/api-de-dados/"):
        raise ValueError("URL deve começar com /api-de-dados/")
    if "API_KEY" not in os.environ or "API_BASE_URL" not in os.environ:
        raise EnvironmentError("Variáveis de ambiente API_KEY e API_BASE_URL são obrigatórias")
    if "pagina" not in params or not isinstance(params["pagina"], int) or params["pagina"] < 1:
        raise ValueError("Parâmetro 'pagina' deve ser informado e deve ser um inteiro positivo")
    
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

def salvar_json_schema(dados: dict, destino: Path) -> None:
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
    PATH_INVENTARIO.parent.mkdir(parents=True, exist_ok=True)
    PATH_INVENTARIO.write_text(json.dumps(list(inventario)))

    return novos

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