# Solemne 2 Big Data — Equipo y organización

## Integrantes
- Benjamín Lagos (Scrum Master)
- Iván Herrera
- Felipe González
- María Alejandra Astrelli
- Adrián Mancilla Carter
- Benjamín Díaz

## Objetivo del proyecto
Implementar un flujo Python → Kafka → PySpark que simule la transmisión de lecturas de la
estación de monitoreo de calidad del aire de Las Condes y analice cómo se comporta la
concentración de CO según la temperatura, la humedad y la hora del día.
El caso, las preguntas y la estructura de los eventos están en `docs/contrato_evento.md`.

## Distribución del trabajo
- *Benjamín Lagos:* Coordinación, contrato del evento (`docs/contrato_evento.md`), README, integración de entregables, grabación del video de la demo y revisión final. Diapositivas 1, 2 y 8.
- *Iván Herrera:* Entorno Kafka (`kafka/docker_compose.yml`, `kafka/crear_topico.sh`), investigación de Kafka (`kafka/investigacion_kafka.md`) y guion de la demo junto a María Alejandra. Diapositiva 3.
- *Felipe González:* Productor Python (`src/productor.py`), incluido el archivo de prueba `data/eventos_prueba.jsonl` para que el equipo de Spark avance. Diapositiva 4.
- *María Alejandra Astrelli:* Consumidor Python (`src/consumidor.py`), archivo completo `data/eventos.jsonl` y muestra `data/eventos_demo.jsonl`. Guion de la demo junto a Iván. Diapositiva 5.
- *Adrián Mancilla Carter:* Carga y preparación con PySpark (`src/preparacion.py`). Diapositivas 5 (parte Spark) y 6.
- *Benjamín Díaz:* Análisis exploratorio y gráficos con PySpark (`src/analisis.py`). Diapositivas 7 y 9.

## Estructura del repositorio
* `/data`: archivos JSON Lines recibidos por el consumidor.
* `/docs`: contrato del evento y organización del equipo.
* `/kafka`: configuración de Kafka e investigación.
* `/src`: productor, consumidor y scripts de PySpark.
* `/outputs/graficos`: visualizaciones del análisis.
* `/presentacion`: presentación y enlace al video de la demo.
