#!/usr/bin/env bash
# Lê os eventos de pedidos direto do Kafka.
# Uso: bash scripts/02_consumir_topico.sh [tópico] [modo]
#   tópico  padrão: shopvibe.shopvibe.orders
#   modo    "historico" (padrão): lista tudo desde o início e continua esperando
#           "novos": mostra só os eventos que chegarem a partir de agora (ideal para o vídeo)
# Para sair: Ctrl+C
TOPICO="${1:-shopvibe.shopvibe.orders}"
MODO="${2:-historico}"
 
ARGS=(--bootstrap-server kafka:9092 --topic "$TOPICO" --property print.timestamp=true)
if [ "$MODO" = "historico" ]; then
  ARGS+=(--from-beginning)
fi
 
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh "${ARGS[@]}"
 