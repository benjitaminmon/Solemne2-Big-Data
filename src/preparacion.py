import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when
import os
import re


def configurar_java():
    """Spark 4.x requiere Java 17 o 21. En Windows, si JAVA_HOME no apunta a una de esas
    versiones, busca un JDK 17/21 instalado en las rutas habituales y lo usa para esta ejecución."""
    if os.name != "nt":
        return
    if re.search(r"(jdk|zulu)[-_]?(17|21)", os.environ.get("JAVA_HOME", ""), re.IGNORECASE):
        return
    candidatos = []
    for base in [r"C:\Program Files\Eclipse Adoptium", r"C:\Program Files\Java",
                 r"C:\Program Files\Microsoft", r"C:\Program Files\Zulu"]:
        if os.path.isdir(base):
            for carpeta in sorted(os.listdir(base), reverse=True):
                if re.match(r"(jdk|zulu)[-_]?(17|21)", carpeta, re.IGNORECASE):
                    candidatos.append(os.path.join(base, carpeta))
    if candidatos:
        os.environ["JAVA_HOME"] = candidatos[0]
        os.environ["PATH"] = os.path.join(candidatos[0], "bin") + os.pathsep + os.environ.get("PATH", "")
        print(f"[*] Usando Java: {candidatos[0]}")
    else:
        print("[ADVERTENCIA] No se encontró Java 17 o 21. Spark puede fallar con versiones más nuevas.")


# Se ejecuta al importar el módulo, antes de crear la sesión de Spark
# (analisis.py importa este archivo antes de iniciar Spark)
configurar_java()

def preparar(ruta_archivo):
    print("\n--- 1. Iniciando sesión de PySpark ---")
    # driver.host/bindAddress = localhost: evita fallos en PCs cuyo nombre tiene
    # caracteres no válidos para Spark (tildes, ñ, espacios)
    spark = SparkSession.builder \
        .appName("Solemne2_Preparacion_Adrian") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    
    #Reduce verbosidad de logs para no ensuciar la terminal en la demo
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n--- 2. Cargando datos desde {ruta_archivo} ---")
    df = spark.read.json(ruta_archivo)

    print("\n--- 3. Esquema Inicial Inferido ---")
    df.printSchema()
    
    conteo_inicial = df.count()
    print(f"Cantidad de registros antes del tratamiento: {conteo_inicial}")

    print("\n--- 4. Comprobación y conversión de tipos ---")
    #Convierte el timestamp ISO 8601 a TimestampType real
    df_tipos = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
    
    #Asegurar el tipo numérico double para las variables de análisis
    columnas_numericas = ["co_ppm", "temperatura_c", "humedad_pct", "velocidad_viento_ms"]
    for c in columnas_numericas:
        df_tipos = df_tipos.withColumn(c, col(c).cast("double"))
        
    print("Esquema tras corregir tipos (Casteo de fecha y numéricos):")
    df_tipos.printSchema()

    print("\n--- 5. Tratamiento del Problema de Calidad ---")
    print("Justificación: Se eliminan los registros con co_ppm nulo (anomalía del sensor)")
    print("ya que es la variable objetivo. Imputarla alteraría los promedios y el análisis temporal.")
    
    df_limpio = df_tipos.na.drop(subset=["co_ppm"])
    
    conteo_final = df_limpio.count()
    registros_eliminados = conteo_inicial - conteo_final
    print(f"Cantidad de registros después del tratamiento: {conteo_final}")
    print(f"Total de registros nulos eliminados: {registros_eliminados}")
    
    print("\n--- 6. Creación de variables categóricas (Requisito para el análisis) ---")
    df_final = df_limpio.withColumn(
        "rango_temp",
        when(col("temperatura_c") < 10, "Frío (< 10 °C)")
        .when((col("temperatura_c") >= 10) & (col("temperatura_c") <= 20), "Templado (10–20 °C)")
        .otherwise("Cálido (> 20 °C)")
    ).withColumn(
        "rango_humedad",
        when(col("humedad_pct") < 40, "Seca (< 40%)")
        .when((col("humedad_pct") >= 40) & (col("humedad_pct") <= 70), "Moderada (40–70%)")
        .otherwise("Húmeda (> 70%)")
    )
    
    print("Esquema final listo para análisis:")
    df_final.printSchema()
    
    return df_final

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Preparación de datos con PySpark para Solemne 2")
    parser.add_argument(
        "--entrada", 
        type=str, 
        default="data/eventos.jsonl", 
        help="Ruta del archivo JSONL a procesar"
    )
    args = parser.parse_args()
    
    #Ejecutamos directamente para ver la línea exacta que falla (Esto fue para arreglar un error que daba al ejecutar el script)
    df_listo = preparar(args.entrada)
    print("\n--- Muestra de 5 registros limpios y enriquecidos ---")
    df_listo.show(5, truncate=False)

    # Cierra la sesión de Spark de forma ordenada (evita avisos de procesos Java en Windows)
    df_listo.sparkSession.stop()
