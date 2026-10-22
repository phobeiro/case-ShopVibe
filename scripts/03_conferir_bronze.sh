#!/usr/bin/env bash
# Mostra o que está gravado na camada bronze (SeaweedFS) e o conteúdo do arquivo mais recente.
# Uso: bash scripts/03_conferir_bronze.sh [tabela]   (padrão: orders)
set -euo pipefail

TABELA="${1:-orders}"
FILER="http://localhost:8888"

echo ">> Arquivos em s3://bronze/${TABELA}/ (via filer)"
DIRS=$(curl -fsS -H "Accept: application/json" "${FILER}/buckets/bronze/${TABELA}/" \
  | grep -o '"FullPath":"[^"]*"' | cut -d'"' -f4 || true)
if [ -z "$DIRS" ]; then
  echo "   (nada ainda - confira: docker logs bronze-writer)"; exit 0
fi

ARQUIVOS=""
for d in $DIRS; do
  ARQUIVOS="$ARQUIVOS
$(curl -fsS -H "Accept: application/json" "${FILER}${d}/" \
   | grep -o '"FullPath":"[^"]*\.jsonl"' | cut -d'"' -f4 || true)"
done
echo "$ARQUIVOS" | sed '/^$/d' | sort

ULTIMO=$(echo "$ARQUIVOS" | sed '/^$/d' | sort | tail -n 1)
echo
echo ">> Conteúdo do arquivo mais recente: ${ULTIMO}"
curl -fsS "${FILER}${ULTIMO}"