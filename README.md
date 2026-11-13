# Solemne 02 — Generación, ingesta y análisis de eventos con Python, Kafka y Spark

## Dependencias y versiones
- Python:
- Docker:
- Java (para PySpark):
- Librerías: ver `requirements.txt`

## Configuración de conexión y tópico
- Servidor Kafka (bootstrap servers): `localhost:9092`
- Tópico: ``

## Orden y comandos de ejecución
1. Levantar Kafka: `docker compose -f kafka/docker_compose.yml up -d`
2. Crear el tópico: `bash kafka/crear_topico.sh`
3. Iniciar el consumidor: `python src/consumidor.py --salida data/eventos.jsonl`
4. Ejecutar el productor (otra terminal): `python src/productor.py --cantidad 5000`
5. Preparación: `python src/preparacion.py --entrada data/eventos.jsonl`
6. Análisis: `python src/analisis.py --entrada data/eventos.jsonl`

Demo (muestra): mismos pasos con `--cantidad 150` y `data/eventos_demo.jsonl`.

## Ubicación de los datos
- Archivo completo (5.000+ eventos): `data/eventos.jsonl`
- Muestra de la demo: `data/eventos_demo.jsonl`
- Gráficos: `outputs/graficos/`
