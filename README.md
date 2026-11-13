<!--
Pendiente: completar versiones de Python, Java y Docker usadas y probar los comandos.
-->

# Solemne 2 Big Data — Generación, ingesta y análisis de eventos con Python, Kafka y Spark

## Dependencias y versiones
- Python:
- Java (requerido por PySpark; versiones 8, 11 o 17):
- Docker Desktop (para levantar Kafka):
- Librerías Python (`requirements.txt`):
  - `pyspark>=3.5.0`
  - `pandas<3`
  - `matplotlib`
  - `kafka-python>=2.0.3`

## Configuración de conexión y tópico
- Servidor Kafka (bootstrap servers): `localhost:9092`
- Tópico: `estacion-lascondes`

## Orden y comandos de ejecución
0. Crear y activar el entorno virtual e instalar dependencias (Windows PowerShell):
   ```powershell
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```
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
