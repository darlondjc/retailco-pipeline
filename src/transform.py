"""
Sprint 2 — Transformação de Dados (transform.py)
=========================================
Responsável por fazer a limpeza dos dados da camada RAW para a camada TRUSTED.
Um exemplo dessa etapa é a estratégia EPA (exploração, polimento e análise de anomalias)

REGRAS:
  - Este script deve ser IDEMPOTENTE (rodar várias vezes sem duplicar dados).
  - Credenciais vêm do .env, nunca hardcoded aqui.
  - Saída: data/trusted/<nome_arquivo>.<extensão>
"""
import pandas as pd

df = pd.read_json("data/raw/permissionarios.json")
print(df.head())