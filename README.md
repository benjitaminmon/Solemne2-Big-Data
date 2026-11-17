# Solemne 2 Big Data — Generación, ingesta y análisis de eventos con Python, Kafka y Spark

## Dependencias y versiones

- Python 3.12 (PySpark y pandas aún no son compatibles con Python 3.15)
- Java 17 o 21 (requerido por PySpark 4.x)
- Docker Desktop con `docker compose` (para levantar Kafka 3.9.1)
- Librerías Python (`requirements.txt`):
  - `pyspark>=3.5.0` (se instala la 4.x)
  - `pandas<3`
  - `matplotlib`
  - `kafka-python>=2.0.3`

## Configuración de conexión y tópico

- Servidor Kafka (bootstrap servers): `localhost:9092`
- Tópico principal: `estacion-lascondes` (conjunto completo, 5.000 eventos)
- Tópico de demostración: `estacion-lascondes-demo` (muestra de 150 eventos)

## Orden y comandos de ejecución (Windows PowerShell, desde la carpeta del proyecto)

### 1. Entorno virtual (una sola vez)

```powershell
py -3.12 -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Cada terminal nueva debe activar el entorno con `.\venv\Scripts\Activate.ps1` (la línea debe empezar con `(venv)`).
Si PowerShell bloquea la activación: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

### 2. Levantar Kafka y preparar los tópicos

Con Docker Desktop abierto:

```powershell
docker compose -f .\kafka\docker_compose.yml up -d
docker ps
.\kafka\crear_topico.bat --reset
```

`--reset` borra y vuelve a crear ambos tópicos vacíos, para que no se acumulen eventos de ejecuciones anteriores. En Linux/Mac: `bash kafka/crear_topico.sh --reset`.

### 3. Generar el conjunto completo (5.000 eventos)

Terminal 1 — iniciar primero el consumidor (queda esperando eventos):

```powershell
python src\consumidor.py --salida data\eventos.jsonl --timeout 20
```

Terminal 2 — ejecutar el productor:

```powershell
python src\productor.py --cantidad 5000 --semilla 42
```

El consumidor guarda cada evento como una línea JSON y se detiene solo tras 8 segundos sin mensajes nuevos (o con Ctrl+C), mostrando el resumen de eventos guardados y errores.

### 4. Preparación y análisis con PySpark

```powershell
python src\preparacion.py --entrada data\eventos.jsonl
python src\analisis.py --entrada data\eventos.jsonl
```

`analisis.py` ejecuta la preparación, responde las tres preguntas y guarda los gráficos en `outputs\graficos\`.

### 5. Muestra para la demostración (150 eventos, separada del conjunto completo)

Terminal 1:

```powershell
python src\consumidor.py --topico estacion-lascondes-demo --salida data\eventos_demo.jsonl
```

Terminal 2:

```powershell
python src\productor.py --cantidad 150 --topico estacion-lascondes-demo
```

Luego:

```powershell
python src\preparacion.py --entrada data\eventos_demo.jsonl
python src\analisis.py --entrada data\eventos_demo.jsonl
```

Los gráficos de la muestra se guardan en `outputs\graficos\demo\`, sin sobrescribir los del conjunto completo.

### 6. Detener Kafka

```powershell
docker compose -f .\kafka\docker_compose.yml down
```

Esto elimina el contenedor y los mensajes del tópico; los archivos de `data\` no se ven afectados.

## Ubicación de los datos

- Conjunto completo (5.000 eventos): `data/eventos.jsonl`
- Muestra de la demostración: `data/eventos_demo.jsonl`
- Gráficos del conjunto completo: `outputs/graficos/`
- Gráficos de la muestra de demostración: `outputs/graficos/demo/`
