# GamePrice Analytics

Pipeline de dados completo para análise de preços e promoções de jogos digitais,
construído sobre a **Medallion Architecture (Bronze → Silver → Gold)**.

---

## Objetivo

Transformar dados brutos de promoções de jogos em informações confiáveis para análise de negócio,
respondendo perguntas como:

- Quais jogos possuem os maiores descontos hoje?
- Quais gêneros têm as melhores promoções?
- Quais desenvolvedores aparecem com mais frequência em promoções?
- Qual o histórico de preços de um jogo ao longo do tempo?

---

## Arquitetura

```
CheapShark API ──┐
                 ├──► Bronze (raw + enriched) ──► Silver (tratado) ──► Gold (analítico) ──► PostgreSQL ──► Dashboard
Steam API ───────┘
```

### Camadas

| Camada | Formato | Descrição |
|--------|---------|-----------|
| **Bronze Raw** | JSON | Dados brutos da CheapShark |
| **Bronze Enriched** | JSON | Dados da CheapShark enriquecidos com Steam |
| **Silver** | Parquet | Dados tratados, tipados e particionados por data |
| **Gold** | Parquet | Tabelas analíticas prontas para consumo |
| **PostgreSQL** | Tabelas relacionais | Destino final para o dashboard |

> **Próximo passo:** migrar Bronze/Silver/Gold do filesystem local para **MinIO** (object storage).

---

## Diagrama do pipeline

<!-- ![Diagrama do pipeline](docs/images/pipeline.png) -->
> *Imagem do diagrama arquitetural — adicionar em `docs/images/`*

---

## Tecnologias

| Componente | Tecnologia |
|---|---|
| Extração | Python + Requests |
| Transformação | Apache Spark (PySpark) |
| Armazenamento analítico | Parquet (filesystem / MinIO) |
| Banco de dados | PostgreSQL |
| Dashboard | Streamlit + Plotly |
| Orquestração (dev/test) | `pipeline.py` / `main.py` |
| Orquestração (prod) | Apache Airflow |
| Testes integrados | `testes/teste_integrado/test_pipeline.py` |

---

## Fluxo do pipeline

### 1. Extração (Bronze)
```bash
python src/extracao/extrair.py
```
- Busca ofertas na **CheapShark API**
- Enriquece com detalhes da **Steam API** (preço, gêneros, desenvolvedores)
- Salva: `data/bronze/raw/` e `data/bronze/enriched/`

### 2. Transformação Silver
```bash
python src/silver/transformar_silver.py
```
- Lê Bronze Enriched
- Aplica tipagem, limpeza e regras de qualidade
- Salva: `data/silver/jogos_precos/`, `jogos_generos/`, `jogos_desenvolvedores/`
- **Particionado por** `data_ingestao`

### 3. Geração Gold
```bash
python src/gold/historico_precos.py
python src/gold/ofertas.py
python src/gold/ranking_desenvolvedores.py
python src/gold/ranking_generos.py
```
- Cria tabelas analíticas a partir da Silver
- **Reprocessável:** remove e recria a partição do dia se rodar novamente

### 4. Carga PostgreSQL
```bash
python src/database/criar_tabelas.py   # primeira vez ou mudança de schema
python src/database/carregar_gold_postgres.py
```

---

## Ambientes

| Ambiente | Config | Orquestração |
|---|---|---|
| `dev` | `.env.dev` | `main.py` ou scripts individuais |
| `test` | `.env.test` | `testes/teste_integrado/test_pipeline.py` |
| `prod` | `.env.prod` | Apache Airflow (`dags/dag_pipeline.py`) |

Para selecionar o ambiente:
```bash
ENVIRONMENT=prod python src/main.py
```

> O arquivo `configuracao/config.py` carrega automaticamente o `.env` correspondente.

---

## ⚠️ Regras e cuidados

### Qualidade dos dados (Silver)
- Apenas jogos com `steam_app_id` válido são mantidos
- Apenas moedas `BRL` e `USD` da Steam são aceitas
- Jogos duplicados por `steam_app_id + data_ingestao` são removidos

### Moedas
- `preco_oferta` e `preco_normal` → sempre **USD** (CheapShark)
- `preco_steam` e `preco_steam_original` → **BRL ou USD** dependendo da conta Steam (verificar `moeda_steam`)
- `economia` na tabela `fato_ofertas` → calculada **somente quando `moeda_steam = USD`**

### Reprocessamento
- As camadas Silver e Gold são **reprocessáveis no mesmo dia** — a partição existente é removida e recriada
- `fato_historico_precos` tem constraint `UNIQUE(steam_app_id, data_ingestao)` no PostgreSQL

### Schema do banco
- O `criar_tabelas.py` faz `DROP TABLE CASCADE` antes de recriar — **não usar em produção sem backup**
- Em produção, usar migrations controladas (ex: Flyway ou Alembic)

---

## 📁 Estrutura do projeto

```
game-price-analytics/
├── configuracao/        # Config, logging, HTTP
├── dags/                # DAGs Airflow (prod)
├── dashboard/           # Streamlit app
├── data/                # Dados locais (Bronze/Silver/Gold)
├── docs/                # Documentação complementar
├── src/
│   ├── extracao/        # CheapShark + Steam APIs
│   ├── bronze/          # Persistência Bronze
│   ├── silver/          # Transformações Silver
│   ├── gold/            # Agregações Gold
│   ├── database/        # Conexão e carga PostgreSQL
│   ├── main.py          # Entrypoint dev
│   └── pipeline.py      # Pipeline completo (dev/test)
└── testes/
    └── teste_integrado/ # Testes end-to-end
```

---
## Tabelas Gold (PostgreSQL)

### `fato_ofertas`
Visão analítica das promoções do dia. Uma linha por jogo por data de ingestão.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `nome` | texto | Nome do jogo |
| `loja` | texto | Loja que está com a promoção (ex: Steam, Humble Store, GOG) |
| `preco_oferta` | numérico | Preço promocional na loja — **sempre USD** (CheapShark) |
| `preco_normal` | numérico | Preço sem desconto na loja — **sempre USD** (CheapShark) |
| `moeda_oferta` | texto | Sempre `USD` |
| `desconto` | numérico | Percentual de desconto calculado pelo CheapShark (0–100) |
| `avaliacao` | texto | Avaliação textual dos usuários na Steam (ex: "Very Positive") |
| `avaliacao_percentual` | inteiro | Percentual de recomendação dos usuários (0–100) |
| `preco_steam` | numérico | Preço atual na Steam — moeda depende de `moeda_steam` |
| `preco_steam_original` | numérico | Preço cheio na Steam antes do desconto |
| `moeda_steam` | texto | Moeda do preço Steam (`BRL` ou `USD`) |
| `economia` | numérico | Diferença `preco_steam - preco_oferta` — **só calculada quando `moeda_steam = USD`** |
| `nivel_oferta` | texto | Classificação do desconto: `EXCELENTE` (≥75%) / `BOA` (≥50%) / `MEDIA` (≥25%) / `BAIXA` |
| `data_ingestao` | data | Data do snapshot |

---

### `fato_historico_precos`
Snapshot diário de preços para acompanhar evolução ao longo do tempo.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `nome` | texto | Nome do jogo |
| `preco_oferta` | numérico | Preço promocional no dia — **USD** |
| `preco_normal` | numérico | Preço sem desconto no dia — **USD** |
| `preco_steam` | numérico | Preço na Steam no dia — moeda depende de `moeda_steam` |
| `moeda_steam` | texto | Moeda do preço Steam (`BRL` ou `USD`) |
| `desconto` | numérico | Percentual de desconto no dia |
| `data_ingestao` | data | Data do snapshot (chave única com `steam_app_id`) |

---

### `ranking_generos`
Métricas agregadas por gênero para o snapshot mais recente.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `genero_nome` | texto | Nome do gênero (ex: Action, RPG, Sports) |
| `quantidade_jogos` | inteiro | Quantidade de jogos distintos em promoção nesse gênero |
| `desconto_medio` | numérico | Desconto médio dos jogos do gênero (%) |
| `preco_medio_oferta` | numérico | Preço médio de oferta — **USD** |
| `preco_medio_normal` | numérico | Preço médio sem desconto — **USD** |
| `maior_desconto` | numérico | Maior desconto encontrado no gênero (%) |
| `avaliacao_media` | numérico | Média do percentual de aprovação dos usuários (0–100) |

---

### `ranking_desenvolvedores`
Métricas agregadas por desenvolvedor para o snapshot mais recente.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `desenvolvedor_nome` | texto | Nome do desenvolvedor/estúdio |
| `quantidade_jogos` | inteiro | Quantidade de jogos distintos em promoção |
| `desconto_medio` | numérico | Desconto médio dos jogos do desenvolvedor (%) |
| `preco_medio_oferta` | numérico | Preço médio de oferta — **USD** |
| `preco_medio_normal` | numérico | Preço médio sem desconto — **USD** |
| `maior_desconto` | numérico | Maior desconto encontrado entre os jogos (%) |
| `avaliacao_media` | numérico | Média do percentual de aprovação dos usuários (0–100) |

---
## Próximos passos

- [ ] Migrar camadas Bronze/Silver/Gold para **MinIO**
- [ ] `pipeline.py` como entrypoint unificado (dev + teste integrado)
- [ ] Testes integrados usando `pipeline.py` com `.env.test`
- [ ] Airflow em produção orquestrando `pipeline.py`

---

## 📖 Documentação complementar

- [Dashboard — guia de uso e visualizações](docs/doc_dash.md)
