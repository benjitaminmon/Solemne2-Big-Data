# =============================================================================
# TAREA — Responsable: Felipe González            (BORRAR ESTE BLOQUE AL TERMINAR)
# -----------------------------------------------------------------------------
# Productor Python que genera eventos y los publica en el tópico de Kafka.
#
# Debe:
#   [ ] Generar y publicar al menos 5.000 eventos en formato JSON.
#   [ ] Respetar exactamente los campos definidos en docs/contrato_evento.md
#       (id del evento, marca temporal, variable categórica, variable numérica
#       y un atributo adicional).
#   [ ] Incluir el PATRÓN intencional definido en el contrato.
#   [ ] Incluir la ANOMALÍA controlada (nulos o fuera de rango) y documentar
#       cómo se introdujo (ej: "3% de los eventos con monto = None").
#   [ ] Recibir la cantidad de eventos por parámetro:
#           python src/productor.py --cantidad 5000
#           python src/productor.py --cantidad 150      (para el video)
#   [ ] Opción para escribir directo a un archivo SIN Kafka, para que Adrián y
#       Benjamín D. avancen con Spark mientras Kafka no está listo:
#           python src/productor.py --cantidad 500 --solo-archivo data/eventos_prueba.jsonl
#   [ ] Informar al final: cantidad de eventos enviados y errores detectados.
#
# Conexión (debe coincidir con consumidor.py y kafka/crear_topico.sh):
#   BOOTSTRAP_SERVERS = "localhost:9092"   -> dirección del servidor Kafka
#   TOPIC             = "<nombre-del-topico>"
#
# PRIORIDAD: es lo primero que necesita el equipo. Entregar el JSONL de prueba
# lo antes posible.
# =============================================================================
