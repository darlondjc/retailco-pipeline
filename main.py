from pathlib import Path

from src.ingest import (
    salvar_com_rastreamento,
    carregar_inventario,
    requisitar_resiliente,
    salvar_idempotente,
)

# Relação de ocupantes de imóveis funcionais (permissionários)
dados_permissionarios = requisitar_resiliente(
    url="/api-de-dados/permissionarios",
    params={"pagina": 1}
)
print(dados_permissionarios)
salvar_com_rastreamento(dados_permissionarios, Path("dados/permissionarios.json"))

# -------------------------------------------------------------------------------------------------------

# Situações dos imóveis funcionais
dados_situacao_imovel = requisitar_resiliente(
    url="/api-de-dados/situacao-imovel",
    params={}
)
print(dados_situacao_imovel)
inventario = carregar_inventario()
salvar_idempotente(dados_situacao_imovel, Path("dados/situacao-imovel.json"), inventario)