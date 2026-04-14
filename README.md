# Projeto retaico-pipeline

Pipeline de ingestão em Python para coleta de dados da API de dados, com foco em confiabilidade de carga, idempotência por registro e controle de execução incremental.

## Fonte de dados

A fonte escolhida é a API de dados do Portal da Transparência acessada por rotas sob o prefixo `/api-de-dados/`.

No estado atual do projeto, o pipeline consome o endpoint de permissionários com paginação:

- `/api-de-dados/permissionarios`

## Decisões de implementação

### Controle de protocolo HTTP

O acesso e as validações do protocolo HTTP foi centralizado na função `requisitar_resiliente`.

Essa função aplica:

- validação da URL para garantir o prefixo `/api-de-dados/`
- validação das credenciais obrigatórias em variáveis de ambiente
- validação do parâmetro obrigatório `pagina`
- timeout por requisição
- tratamento explícito de `401` e `403` sem retentativa
- tratamento de `429` com uso do header `Retry-After`
- retentativas com backoff exponencial para erros `5xx`

Com isso, o pipeline falha cedo em erros de configuração e se recupera de falhas transitórias de rede ou indisponibilidade da API.

### Estratégia de carga Full Load

A carga adotada para permissionários é Full Load paginado, devido ao pequeno tamanho da carga de dados (<5G)

O processo começa na página `1` e continua até que uma página retorne sem registros. Isso evita depender de um total prévio informado pela API e simplifica o controle de parada.

### Idempotência

A idempotência foi implementada por hash de registro, e não por hash do lote inteiro.

Cada registro é serializado com `json.dumps(..., sort_keys=True)` e transformado em hash SHA-256. O inventário de hashes já vistos é mantido em:

- `data/control/hashes.json`

Na prática, isso permite:

- evitar duplicação de registros em reprocessamentos
- manter checkpoint incremental entre execuções
- continuar a carga por páginas sem regravar o que já foi persistido

### Raw imutável

O dado bruto persistido em JSON não deve ser reescrito de forma destrutiva para remover ou modificar registros já coletados. O método idempotente acrescenta somente novos registros ao arquivo de saída.

Além disso, o schema é salvo separadamente em arquivo `.schema.json`, sem duplicar a responsabilidade de persistência do dado bruto.

### Watermark

O pipeline mantém um watermark em:

- `data/control/watermark.json`

Esse arquivo registra:

- `ultimo_registro`
- `atualizado_em`

O objetivo é suportar controle temporal de execução e servir de base para evoluções futuras de carga incremental orientada por tempo.

### Hashes de registros

Os hashes são usados como mecanismo de deduplicação técnica. O controle é separado do dado bruto para que a lógica de idempotência fique explícita e auditável.

### Segurança de credenciais

As credenciais não ficam hardcoded no código.

O projeto usa `python-dotenv` para carregar variáveis a partir de um arquivo `.env` na raiz. As variáveis esperadas são:

- `API_KEY`
- `API_BASE_URL`

O `.gitignore` já está configurado para impedir versionamento do `.env`, reduzindo o risco de vazamento de credenciais.

## Estrutura de artefatos

Arquivos gerados e usados pelo pipeline:

- `data/raw/permissionarios.json`: dataset bruto acumulado com somente novos registros
- `data/raw/permissionarios.schema.json`: metadados de schema do dataset coletado
- `data/control/hashes.json`: inventário de hashes dos registros já persistidos
- `data/control/watermark.json`: controle temporal da última atualização

## Instrução de execução

### 1. Configurar ambiente

Crie um arquivo `.env` na raiz do projeto com:

```env
API_KEY=sua_chave
API_BASE_URL=https://sua-url-base
```

### 2. Instalar dependências

Se estiver usando `uv`:

```bash
uv sync
```

### 3. Executar o pipeline

```bash
uv run python main.py
```

## Comportamento esperado da execução

Durante a execução, o pipeline:

- inicia a ingestão de permissionários
- percorre as páginas sequencialmente
- persiste somente registros novos
- atualiza o schema do dataset
- atualiza o watermark de controle
- exibe a quantidade total de registros lidos e de novos registros persistidos

## Observações

- O método de requisição atual exige o parâmetro `pagina`, portanto está acoplado ao fluxo paginado atual.
- Se novos endpoints sem paginação forem incorporados, o ideal é separar a função HTTP genérica da função HTTP paginada.