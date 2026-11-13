#!/bin/bash
# =============================================================================
# TAREA — Responsable: Iván Herrera                (BORRAR ESTE BLOQUE AL TERMINAR)
# -----------------------------------------------------------------------------
# Crea el tópico del proyecto en Kafka y verifica que exista.
# Kafka arranca vacío: este script se ejecuta después de docker compose up.
#
#   [ ] Definir el nombre del tópico (debe coincidir con productor.py y
#       consumidor.py).
#   [ ] Crear el tópico con kafka-topics --create.
#   [ ] Listar los tópicos al final (este comando se muestra en el video).
#   [ ] Si alguien usa Windows sin Git Bash, crear crear_topico.bat equivalente
#       o dejar el comando en el README.
#
# Ejemplo:
#   docker exec kafka kafka-topics --create --topic <nombre-del-topico> \
#       --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
#   docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
# =============================================================================
