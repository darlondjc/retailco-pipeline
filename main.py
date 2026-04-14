from pathlib import Path

from src.ingest import (
    salvar_com_rastreamento,
    carregar_inventario,
    requisitar_resiliente,
    salvar_idempotente,
    carregar_watermark,
    salvar_watermark
)

print("Escolha a opção de ingestão de permissionários:\n1 - Salvar com rastreamento de timestamp e schema;\n2 - Salvar com idempotência usando hash do conteúdo;")
opcao = input("Digite 1 ou 2: ").strip()

if opcao not in {"1", "2"}:
    print("Opção inválida. Saindo...")
    exit(1)

# Relação de ocupantes de imóveis funcionais (permissionários)
inventario = carregar_inventario()
dados_permissionarios = []
destino_permissionarios = Path("dados/permissionarios.json")
novos = 0
pagina = 1
while True:
    print(f"Lendo página {pagina}...")
    dados_pagina = requisitar_resiliente(
        url="/api-de-dados/permissionarios",
        params={"pagina": pagina}
    )

    if not dados_pagina:
        break

    if isinstance(dados_pagina, list):
        dados_permissionarios.extend(dados_pagina)
    else:
        dados_permissionarios.append(dados_pagina)

    if opcao == "1":
        # Checkpoint incremental: mantém o arquivo atualizado a cada página
        salvar_com_rastreamento(dados_permissionarios, destino_permissionarios)
    elif opcao == "2":
        # Checkpoint incremental: mantém o arquivo atualizado a cada página
        novos += salvar_idempotente(dados_permissionarios, destino_permissionarios, inventario)

    pagina += 1

print(len(dados_permissionarios), "permissionários existentes.")
if opcao == "2":
    print(novos, "novos.")

salvar_watermark(carregar_watermark())
