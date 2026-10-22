#!/usr/bin/env bash
# ShopVibe - Etapas 1 e 2: cria a origem no Postgres e registra o conector Debezium.
# Rode a partir da pasta do docker-compose.yml:  bash scripts/01_setup_origem_e_conector.sh
set -euo pipefail

CONNECT_URL="http://localhost:8083"

echo ">> 1/4 Criando schema e tabelas no Postgres"
docker exec -i postgres psql -U postgres -d postgres -v ON_ERROR_STOP=1 < sql/01_schema.sql

echo ">> 2/4 Inserindo clientes e produtos"
docker exec -i postgres psql -U postgres -d postgres -v ON_ERROR_STOP=1 < sql/02_seed.sql

echo ">> 3/4 Aguardando o Kafka Connect ficar pronto"
for i in $(seq 1 60); do
  if curl -fsS "$CONNECT_URL/connectors" >/dev/null 2>&1; then break; fi
  echo "   aguardando... ($i)"; sleep 3
done

echo ">> 4/4 Registrando o conector (recria se já existir)"
curl -fsS -X DELETE "$CONNECT_URL/connectors/shopvibe-postgres" >/dev/null 2>&1 || true
curl -fsS -X POST -H "Content-Type: application/json" \
  --data @connectors/shopvibe-postgres.json "$CONNECT_URL/connectors"
echo

sleep 5
echo ">> Status do conector"
curl -fsS "$CONNECT_URL/connectors/shopvibe-postgres/status"
echo
echo ">> Tópicos shopvibe.*"
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list | grep shopvibe || true
