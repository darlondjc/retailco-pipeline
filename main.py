from pathlib import Path
from datetime import datetime
from src.util import obter_config_api
from src.ingest import (
    salvar_json_schema,
    carregar_inventario,
    requisitar_resiliente,
    salvar_idempotente,
    carregar_watermark,
    salvar_watermark
)

obter_config_api()

print("Iniciando ingestão de permissionários...")

# Relação de ocupantes de imóveis funcionais (permissionários)
inventario = carregar_inventario()
dados_permissionarios = []
destino_permissionarios = Path("data/raw/permissionarios.json")
novos = 0
pagina = 1
while True:
    print(f"Lendo página {pagina}...")
    dados_pagina = requisitar_resiliente(
        url="/permissionarios",
        params={"pagina": pagina}
    )

    if not dados_pagina:
        break

    if isinstance(dados_pagina, list):
        dados_permissionarios.extend(dados_pagina)
    else:
        dados_permissionarios.append(dados_pagina)

    # Checkpoint incremental: mantém o arquivo atualizado a cada página
    novos += salvar_idempotente(dados_permissionarios, destino_permissionarios, inventario)

    pagina += 1

# JSON Schema
referencia_execucao = datetime.now().astimezone().isoformat()
salvar_json_schema(dados_permissionarios, destino_permissionarios, coletado_em=referencia_execucao)

print(len(dados_permissionarios), "permissionários existentes.")
print(novos, "novos.")

# Atualiza watermark para controle de atualizações incrementais
salvar_watermark(carregar_watermark(), atualizado_em=referencia_execucao)
