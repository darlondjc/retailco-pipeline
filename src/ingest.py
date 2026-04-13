import os, requests, time
from dotenv import load_dotenv

load_dotenv() # carrega .env para os.environ

def requisitar_resiliente(url: str, params: dict, max_tentativas: int = 5) -> dict:
    headers = {"chave-api-dados": os.environ["API_KEY"]}
    url = os.environ["API_BASE_URL"] + url
    #print(f"Requisitando {url} com params {params} ...")
    tentativa = 0
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

        elif r.status_code >= 500:
            espera = 2 ** tentativa
            print(f"[{r.status_code}] Erro servidor. Retry em {espera}s .")
            time.sleep(espera)
        else:
            raise ValueError(
                f"Erro inesperado: {r.status_code} — {r.text[:200]}"
            )
        tentativa += 1

    raise TimeoutError(
        f"Máximo de {max_tentativas} tentativas atingido para {url}"
    )
