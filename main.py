from pathlib import Path

from src.ingest import requisitar_resiliente, salvar_com_rastreamento

# Viagens
dados = requisitar_resiliente(
    url="/api-de-dados/viagens",
    params={"dataIdaDe": "01/04/2026", "dataIdaAte": "30/04/2026", "dataRetornoDe": "01/04/2026", "dataRetornoAte": "30/04/2026", "codigoOrgao": "02000", "pagina": 1}
)
print(dados)

# Situações dos imóveis funcionais
dados = requisitar_resiliente(
    url="/api-de-dados/situacao-imovel",
    params={}
)
print(dados)
salvar_com_rastreamento(dados, Path("dados/situacao-imovel.json"))