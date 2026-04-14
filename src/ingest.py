import json
import os
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv() # carrega .env para os.environ

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

        elif r.status_code >= 500: # estratégia de espera exponencial para erros de servidor
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
    # Salva o dado bruto
    destino.write_text(json.dumps(dados, ensure_ascii=False, indent=2))
    # Salva metadado de schema ao lado
    schema = {"campos": list(dados.keys()),
              "n_campos": len(dados),
              "coletado_em": datetime.now().isoformat()}
    destino.with_suffix(".schema.json").write_text(
        json.dumps(schema, ensure_ascii=False))

