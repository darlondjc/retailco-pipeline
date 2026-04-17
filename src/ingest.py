"""
Sprint 1 — Ingestão de Dados (ingest.py)
=========================================
Responsável por coletar dados da fonte (API/CSV/etc.) e
salvar na camada RAW (imutável, dado bruto).

REGRAS:
  - Este script deve ser IDEMPOTENTE (rodar várias vezes sem duplicar dados).
  - Credenciais vêm do .env, nunca hardcoded aqui.
  - Saída: data/raw/<nome_arquivo>.<extensão>
"""

import json
import os
import hashlib
import random
import time
import requests

from datetime import datetime, timezone
from pathlib import Path
from util import obter_dados_api

# ── Configuração ──────────────────────────────────────────────────────────────
obter_dados_api()  # Carrega .env e valida variáveis de ambiente

RAW_FILE = Path("data/raw/permissionarios.json")
RAW_FILE.parent.mkdir(parents=True, exist_ok=True)

HASH_FILE = Path("data/control/hashes.json")
HASH_FILE.parent.mkdir(parents=True, exist_ok=True)

WATERMARK_FILE = Path("data/control/watermark.json")
WATERMARK_FILE.parent.mkdir(parents=True, exist_ok=True)

# ── Funções ───────────────────────────────────────────────────────────────────

def carregar_hash_inventario() -> set:
    if HASH_FILE.exists():
        conteudo = HASH_FILE.read_text().strip()
        if conteudo:
            return set(json.loads(conteudo))
    return set() # 1a execução ou arquivo vazio

def fetch_data_resiliente(endpoint: str, params: dict | None = None) -> list[dict]:    
    """
    Realiza uma requisição GET ao endpoint da API.
    Retorna uma lista de registros (JSON).
    """

    params = params or {}

    #validações parametros obrigatórios
    if "pagina" not in params or not isinstance(params["pagina"], int) or params["pagina"] < 1:
        raise ValueError("Parâmetro 'pagina' deve ser informado e deve ser um inteiro positivo")
    
    # chave-api-dados é como a API do Portal da Transparência espera o token de acesso, mas isso pode variar conforme a API alvo
    headers = {"chave-api-dados": os.environ["API_KEY"]}
    url = os.environ["API_BASE_URL"] + endpoint
    #print(f"Requisitando {url} com params {params} ...")

    tentativa = 0
    max_tentativas = 5
    while tentativa < max_tentativas:
        # print(f"[INGEST] Buscando dados em: {url}, {params}, tentativa {tentativa+1}")
        response = requests.get(url, params=params, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.json()

        if response.status_code in (401, 403): # ← nunca retentar
            raise PermissionError(
                f"Falha de auth ({response.status_code}). Revise o token ou o nome da chave especificada na API."
            )

        if response.status_code == 429:
            # O servidor diz quanto tempo esperar — use esse valor
            espera = int(response.headers.get("Retry-After", 2 ** tentativa + random.randint(0, 1)))
            print(f"[429] Rate limit. Aguardando {espera}s .")
            time.sleep(espera)

        elif response.status_code >= 500: # estratégia de backoff exponencial com jitter aleatorio para erros de servidor
            espera = 2 ** tentativa + random.randint(0, 1)
            print(f"[{response.status_code}] Erro servidor. Retry em {espera}s .")
            time.sleep(espera)
        else:
            raise ValueError(
                f"Erro inesperado: {response.status_code} — {response.text[:200]}"
            )
        tentativa += 1
        print(f"Chamadas restantes nesta janela: {max_tentativas}")

    raise TimeoutError(
        f"Máximo de {max_tentativas} tentativas atingido para {url}"
    )

def save_raw(conteudo, destino: Path, inventario: set) -> int:
    """
    Salva os dados brutos em JSON na camada raw com timestamp.
    Estratégia: Full Load com data de coleta no nome do arquivo.
    """
    # Persiste registros sem duplicar e reconstrói o raw se ele tiver sido apagado
    registros = conteudo if isinstance(conteudo, list) else [conteudo]

    destino.parent.mkdir(parents=True, exist_ok=True)
    if destino.exists() and destino.is_file():
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

    #cria arquivo de hash do inventario se não existir
    HASH_FILE.parent.mkdir(parents=True, exist_ok=True)
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
    HASH_FILE.write_text(json.dumps(list(inventario)))

    return novos

def save_json_schema(dados: dict, destino: Path, coletado_em: str | None = None) -> str:
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

def normalize_datetime(valor: str) -> datetime:
    dt = datetime.fromisoformat(valor)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt

def load_watermark() -> str:
    if WATERMARK_FILE.exists():
        conteudo = json.loads(WATERMARK_FILE.read_text())
        return conteudo.get("atualizado_em") or conteudo.get("ultimo_registro", "1970-01-01T00:00:00+00:00")
    # Data zero: na 1a execução faz Full Load implícito
    return "1970-01-01T00:00:00+00:00"

def save_watermark(timestamp: str, atualizado_em: str | None = None) -> None:
    ultimo = normalize_datetime(timestamp)

    if atualizado_em is None:
        atualizado_em = datetime.now(timezone.utc).isoformat()

    coletado = normalize_datetime(atualizado_em)

    WATERMARK_FILE.write_text(
        json.dumps({
            "ultimo_registro": coletado.isoformat(),
            "atualizado_em": coletado.isoformat()
        })
    )

    latencia_min = (coletado - ultimo).total_seconds() / 60
    print(f"Latência do pipeline: {latencia_min:.1f} min")

# ── Ponto de Entrada ──────────────────────────────────────────────────────────

def main():
    hash_inventario = carregar_hash_inventario()
    dados_permissionarios = []
    novos = 0
    pagina = 1
    while True:
        print(f"Lendo página {pagina}...")
        dados_pagina = fetch_data_resiliente(
            endpoint="/permissionarios",
            params={"pagina": pagina}
        )

        if not dados_pagina:
            break

        if isinstance(dados_pagina, list):
            dados_permissionarios.extend(dados_pagina)
        else:
            dados_permissionarios.append(dados_pagina)

        # Checkpoint incremental: mantém o arquivo atualizado a cada página
        novos += save_raw(dados_permissionarios, RAW_FILE, hash_inventario)
        print(f"[INGEST] {novos} novos registros persistidos.")

        pagina += 1

    # JSON Schema
    referencia_execucao = datetime.now().astimezone().isoformat()
    save_json_schema(dados_permissionarios, RAW_FILE, coletado_em=referencia_execucao)

    print(len(dados_permissionarios), "permissionários existentes.")
    print(novos, "novos.")

    # Atualiza watermark para controle de atualizações incrementais
    watermark_anterior = load_watermark()
    save_watermark(watermark_anterior, atualizado_em=referencia_execucao)
    print("[INGEST] ✅ Sprint 1 concluído.")


if __name__ == "__main__":
    main()
