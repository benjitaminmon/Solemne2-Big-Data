#!/bin/bash
# =============================================================================
# Crea los tópicos del proyecto en Kafka y los lista.
# Requisito: Kafka levantado con  docker compose -f kafka/docker_compose.yml up -d
#
# Uso:
#   bash kafka/crear_topico.sh            -> crea los tópicos si no existen y los lista
#   bash kafka/crear_topico.sh --reset    -> BORRA y recrea los tópicos (deja offsets en 0)
#
# Tópicos:
#   estacion-lascondes       -> conjunto completo (5.000+ eventos)
#   estacion-lascondes-demo  -> muestra para el video (100–200 eventos)
# =============================================================================

set -e

CONTENEDOR="kafka"
BROKER="localhost:9092"
TOPICOS=("estacion-lascondes" "estacion-lascondes-demo")
KT="/opt/kafka/bin/kafka-topics.sh"   # ruta del script en la imagen apache/kafka

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTENEDOR"; then
  echo "[ERROR] El contenedor '$CONTENEDOR' no está corriendo."
  echo "        Ejecuta: docker compose -f kafka/docker_compose.yml up -d"
  exit 1
fi

if [ "$1" == "--reset" ]; then
  for t in "${TOPICOS[@]}"; do
    echo "[*] Eliminando tópico $t (si existe)..."
    docker exec "$CONTENEDOR" "$KT" --delete --if-exists --topic "$t" --bootstrap-server "$BROKER"
  done
  sleep 3   # Kafka borra los tópicos de forma asíncrona
fi

for t in "${TOPICOS[@]}"; do
  echo "[*] Creando tópico $t..."
  docker exec "$CONTENEDOR" "$KT" --create --if-not-exists --topic "$t" \
    --bootstrap-server "$BROKER" --partitions 1 --replication-factor 1
done

echo
echo "=== Tópicos disponibles en $BROKER ==="
docker exec "$CONTENEDOR" "$KT" --list --bootstrap-server "$BROKER"

echo
echo "=== Detalle del tópico principal ==="
docker exec "$CONTENEDOR" "$KT" --describe --topic "${TOPICOS[0]}" --bootstrap-server "$BROKER"
