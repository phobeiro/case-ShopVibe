# ShopVibe: ingestão de pedidos em tempo real

Captura dos pedidos do PostgreSQL com Debezium, publicação no Kafka e gravação dos eventos brutos
na camada bronze (SeaweedFS, API S3). Atende RF01 e RF02 do caso de uso.

```
PostgreSQL --WAL--> Debezium (Kafka Connect) --> Kafka --> bronze-writer --> SeaweedFS (bronze)
```

## Estrutura

```
docker-compose.yml          ambiente completo
.env                        versões das imagens
connectors/                 configuração do conector Debezium
sql/                        schema, dados iniciais e o pedido de demonstração
scripts/                    comandos da demonstração
bronze_writer/              writer Kafka -> S3 (Python)
docs/                       roteiro do vídeo (e, na entrega, ADRs e diagrama)
```

## Pré-requisitos

- Docker com Docker Compose v2
- Acesso à internet para baixar imagens e pacotes Python
- Portas livres: 5432, 5433, 8080, 8083, 8333, 8888, 9333, 29092

## `.env`

```env
POSTGRES_DEBEZIUM=3.0
PAGADMIN_VERSION=9.0
DEBEZIUM_VERSION=3.0
KAFKA_VERSION=3.8.0
KAFKA_UI_VERSION=v0.7.2
SEAWEEDFS_VERSION=latest
```

## Comandos (na ordem)

Rode tudo a partir da pasta que contém o `docker-compose.yml`.

```bash
# 1. Subir o ambiente (constrói a imagem do bronze-writer)
docker compose up -d --build
docker compose ps -a

# 2. Criar a origem (schema, tabelas, dados) e registrar o conector Debezium
bash scripts/01_setup_origem_e_conector.sh

# 3. Conferir que o conector está RUNNING
curl -s localhost:8083/connectors/shopvibe-postgres/status

# 4. Terminal A: acompanhar os eventos que chegam ao Kafka (só os novos)
bash scripts/02_consumir_topico.sh shopvibe.shopvibe.orders novos

# 5. Terminal B: criar um pedido novo na origem
docker exec -i postgres psql -U postgres -d postgres < sql/03_novo_pedido.sql

# 6. Conferir a camada bronze (arquivos e conteúdo do mais recente)
bash scripts/03_conferir_bronze.sh
bash scripts/03_conferir_bronze.sh order_items

# 7. Log do writer (opcional)
docker logs --tail 20 bronze-writer
```

## Onde olhar

| O quê | Endereço |
| --- | --- |
| Kafka UI (tópicos e mensagens) | http://localhost:8080 |
| pgAdmin (usuário lab-pgadmin4@pgadmin.org, senha postgres) | http://localhost:5433 |
| Kafka Connect (API) | http://localhost:8083 |
| SeaweedFS filer (arquivos da bronze) | http://localhost:8888/buckets/bronze/ |
| SeaweedFS master | http://localhost:9333 |

Tópicos criados pelo Debezium: `shopvibe.shopvibe.orders` e `shopvibe.shopvibe.order_items`.

Formato na bronze: `bronze/<tabela>/dt=AAAA-MM-DD/<tabela>-p<partição>-<offset inicial>-<offset final>.jsonl`.

## Problemas comuns

- **`bronze-writer` sem arquivos:** confira `docker logs bronze-writer`. Ele só enxerga os tópicos depois que o conector é registrado (passo 2); a descoberta leva alguns segundos.
- **Conector em `FAILED`:** `curl -s localhost:8083/connectors/shopvibe-postgres/status` mostra o erro. Para recriar, rode o passo 2 de novo (ele apaga e registra o conector outra vez).
- **Terminal travado no passo 4:** é o esperado, o consumidor fica esperando eventos. Use `Ctrl+C` para sair.
- **Imagens do MinIO não baixam:** por isso a bronze usa SeaweedFS (ver ADR-003).

## Limpeza

```bash
docker compose down        # para os contêineres, mantém os dados
docker compose down -v     # também apaga os volumes (recomeça do zero)
```

## Limites do laboratório

Um único broker Kafka (fator de replicação 1), S3 sem autenticação e JSON sem schema. A durabilidade e a
escala são argumentadas nos ADRs, não demonstradas aqui.
