# =============================================================================
# TAREA — Responsable: María Alejandra Astrelli    (BORRAR ESTE BLOQUE AL TERMINAR)
# -----------------------------------------------------------------------------
# Consumidor Python que lee los eventos desde Kafka y los guarda en JSON Lines
# (un objeto JSON por línea).
#
# Debe:
#   [ ] Suscribirse al tópico del proyecto.
#   [ ] Recibir y deserializar los eventos (JSON -> dict).
#   [ ] Guardar cada evento en una línea del archivo de salida.
#   [ ] Recibir el archivo de salida por parámetro:
#           python src/consumidor.py --salida data/eventos.jsonl        (completo)
#           python src/consumidor.py --salida data/eventos_demo.jsonl   (video)
#   [ ] Mostrar por pantalla una pequeña muestra de mensajes recibidos
#       (ej: los primeros 5).
#   [ ] Informar la cantidad de eventos guardados al terminar.
#   [ ] Registrar los mensajes que no se puedan interpretar en
#       data/errores_consumidor.log (sin detener el programa).
#   [ ] Definir y poder EXPLICAR cómo inicia y cómo termina la recepción
#       (ej: termina tras X segundos sin mensajes, o con Ctrl+C).
#
# Orden de ejecución: el consumidor se inicia ANTES que el productor.
#
# Conexión (debe coincidir con productor.py y kafka/crear_topico.sh):
#   BOOTSTRAP_SERVERS = "localhost:9092"
#   TOPIC             = "<nombre-del-topico>"
#
# Entregables a cargo: data/eventos.jsonl (5.000+) y data/eventos_demo.jsonl
# (100–200), SIEMPRE en archivos separados.
# =============================================================================
