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

## Flujo de ejecución en Windows
1. Crear y activar el entorno virtual e instalar dependencias (Windows PowerShell):
   ```powershell
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   pip install -r requirements.txt
   ```

2. Levantar Kafka:
   ```powershell
   docker compose -f .\kafka\docker_compose.yml up -d
   ```
3. Crear el tópico del proyecto:
   ```powershell
   docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic estacion-lascondes --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```
4. Generar y publicar 5.000 eventos:
   ```powershell
   python src\productor.py --cantidad 5000
   ```
   Si solo necesitas generar un archivo local, sin Kafka, usa en su lugar:
   ```powershell
   python src\productor.py --cantidad 5000 --solo-archivo data\eventos.jsonl
   ```
   Este modo escribe directamente el JSONL en `data\eventos.jsonl`; no requiere
   levantar Kafka ni crear el tópico. Luego puedes continuar con el paso 6.
5. Exportar los mensajes del tópico a JSON Lines:
   ```powershell
   cmd /c "docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic estacion-lascondes --from-beginning --max-messages 5000 > data\eventos.jsonl"
   ```
6. Preparar y analizar los datos:
   ```powershell
   python src\preparacion.py --entrada data\eventos.jsonl
   python src\analisis.py --entrada data\eventos.jsonl
   ```

Para generar una muestra de demo sin mezclarla con el tópico completo:
```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic estacion-lascondes-demo --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
python src\productor.py --cantidad 150 --topico estacion-lascondes-demo
cmd /c "docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic estacion-lascondes-demo --from-beginning --max-messages 150 > data\eventos_demo.jsonl"
```

`src\consumidor.py` y `kafka\crear_topico.sh` aún no implementan esos pasos; los comandos anteriores usan las herramientas incluidas en la imagen de Kafka. El exportador lee desde el inicio del tópico, por lo que, si se ejecutó el productor varias veces en el mismo tópico, puede ser necesario crear un tópico nuevo para exportar solo una ejecución.

Al publicar, Kafka guarda los mensajes en el registro del tópico dentro del contenedor; eso no crea automáticamente un archivo JSONL en la carpeta `data`. El comando del paso 5 lee esos mensajes y redirige su salida a `data\eventos.jsonl`.

El Compose actual no monta un volumen persistente. Los mensajes permanecen en el contenedor mientras este exista, sujetos a la política de retención de Kafka, pero se pierden al eliminarlo. Exporta los datos antes de ejecutar `docker compose -f .\kafka\docker_compose.yml down`.

## Ubicación de los datos
- Archivo completo (5.000+ eventos): `data/eventos.jsonl`
- Muestra de la demo: `data/eventos_demo.jsonl`
- Gráficos: `outputs/graficos/`
