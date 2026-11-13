# =============================================================================
# TAREA — Responsable: Benjamín Diaz               (BORRAR ESTE BLOQUE AL TERMINAR)
# -----------------------------------------------------------------------------
# Análisis exploratorio con PySpark sobre los datos ya preparados.
#
# Debe:
#   [ ] Recibir la ruta del archivo por parámetro (mismo código para ambos):
#           python src/analisis.py --entrada data/eventos.jsonl        (análisis real)
#           python src/analisis.py --entrada data/eventos_demo.jsonl   (video)
#       Reutilizar la función de preparación de preparacion.py (no duplicarla).
#   [ ] Estadística descriptiva de al menos una variable numérica.
#   [ ] Agrupación o comparación por categoría.
#   [ ] Agrupación temporal (por hora, día, etc.).
#   [ ] Responder explícitamente las 3 preguntas de docs/contrato_evento.md.
#   [ ] 3 gráficos (uno por pregunta), con título, ejes identificados e
#       interpretación breve. Guardarlos en outputs/graficos/:
#           pregunta1.png, pregunta2.png, pregunta3.png
#
# Reglas:
#   - Los cálculos principales se hacen con PySpark. Pandas + matplotlib solo
#     para graficar resultados YA agregados (ej: .toPandas() de un groupBy).
#   - Las conclusiones deben reconocer que los datos son sintéticos y que
#     algunos resultados responden a las reglas de generación.
#   - Los resultados de la presentación salen SOLO del archivo completo
#     (eventos.jsonl), no de la muestra del video.
#
# Diapositivas a cargo: 7 y 9.
# =============================================================================
