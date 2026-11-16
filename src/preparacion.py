import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when

def preparar(ruta_archivo):
    print("\n--- 1. Iniciando sesión de PySpark ---")
    spark = SparkSession.builder \
        .appName("Solemne2_Preparacion_Adrian") \
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
        when(col("temperatura_c") < 10, "Frío")
        .when((col("temperatura_c") >= 10) & (col("temperatura_c") <= 20), "Templado")
        .otherwise("Cálido")
    ).withColumn(
        "rango_humedad",
        when(col("humedad_pct") < 40, "Seca")
        .when((col("humedad_pct") >= 40) & (col("humedad_pct") <= 70), "Moderada")
        .otherwise("Húmeda")
    )
    
    print("Esquema final listo para análisis:")
    df_final.printSchema()
    
    return df_final

if __name__ == "__main__":
    #Configuración de argumentos por línea de comandos
    parser = argparse.ArgumentParser(description="Preparación de datos con PySpark para Solemne 2")
    parser.add_argument(
        "--entrada", 
        type=str, 
        default="data/eventos_prueba.jsonl", 
        help="Ruta del archivo JSONL a procesar"
    )
    args = parser.parse_args()
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Preparación de datos con PySpark para Solemne 2")
    parser.add_argument(
        "--entrada", 
        type=str, 
        default="data/eventos_prueba.jsonl", 
        help="Ruta del archivo JSONL a procesar"
    )
    args = parser.parse_args()
    
    #Ejecutamos directamente para ver la línea exacta que falla (Esto fue para arreglar un error que daba al ejecutar el script)
    df_listo = preparar(args.entrada)
    print("\n--- Muestra de 5 registros limpios y enriquecidos ---")
    df_listo.show(5, truncate=False)