# =============================================================================
# TAREA — Responsable: Adrián Mancilla Carter      (BORRAR ESTE BLOQUE AL TERMINAR)
# -----------------------------------------------------------------------------
# Carga y preparación de los datos con PySpark (NO se lee desde Kafka, solo
# desde el archivo JSONL generado por el consumidor).
#
# Debe:
#   [ ] Recibir la ruta del archivo por parámetro (mismo código para ambos):
#           python src/preparacion.py --entrada data/eventos.jsonl        (análisis real)
#           python src/preparacion.py --entrada data/eventos_demo.jsonl   (video)
#       Mientras no exista el archivo real, usar data/eventos_prueba.jsonl.
#   [ ] Cargar el archivo en un DataFrame de PySpark.
#   [ ] Mostrar el esquema (printSchema).
#   [ ] Contar los registros.
#   [ ] Revisar los tipos de las variables usadas en el análisis y corregirlos
#       si la inferencia no los dejó bien.
#   [ ] Convertir la marca temporal a tipo timestamp.
#   [ ] Identificar y tratar la anomalía definida en docs/contrato_evento.md,
#       con una justificación (¿se eliminan? ¿se imputan? ¿por qué?).
#   [ ] Mostrar la cantidad de registros ANTES y DESPUÉS del tratamiento.
#   [ ] Dejar una función (ej: preparar(ruta) -> DataFrame limpio) que
#       analisis.py pueda importar y reutilizar.
#
# Diapositivas a cargo: 5 (parte Spark) y 6.
# =============================================================================
