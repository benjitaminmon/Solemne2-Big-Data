import argparse
import os
import sys
import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Asegurar importación desde src/ sin importar desde dónde se ejecute el comando
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# La preparación (carga, tipos, limpieza de nulos y rangos) vive SOLO en preparacion.py
from preparacion import preparar  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="Análisis exploratorio con PySpark - Estación Las Condes")
    parser.add_argument("--entrada", required=True, help="Ruta al archivo JSONL de entrada")
    parser.add_argument(
        "--graficos",
        default=None,
        help="Carpeta de salida de los gráficos. Por defecto: outputs/graficos, "
             "o outputs/graficos/demo si el archivo de entrada es la muestra de demostración.",
    )
    args = parser.parse_args()

    # Los gráficos de la muestra de demo se guardan aparte para no sobrescribir los oficiales
    if args.graficos:
        carpeta_graficos = args.graficos
    elif "demo" in os.path.basename(args.entrada).lower():
        carpeta_graficos = os.path.join("outputs", "graficos", "demo")
    else:
        carpeta_graficos = os.path.join("outputs", "graficos")
    os.makedirs(carpeta_graficos, exist_ok=True)
    print(f"[*] Los gráficos se guardarán en: {carpeta_graficos}")

    spark = (
        SparkSession.builder
        .appName("AnalisisEstacionLasCondes")
        .master("local[*]")
        # Evita fallos en PCs cuyo nombre tiene caracteres no válidos para Spark (tildes, ñ)
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Preparación de datos (preparacion.py): esquema, tipos, nulos y rangos
    df = preparar(args.entrada)
    spark.sparkContext.setLogLevel("ERROR")  # preparar() deja WARN; se vuelve a ERROR para una salida limpia

    # 2. Variable derivada propia del análisis temporal (P3)
    df = df.withColumn("hora", F.hour("timestamp"))
    df.cache()

    print("\n" + "=" * 70)
    print(f"ANÁLISIS EXPLORATORIO EN PYSPARK | Archivo: {args.entrada}")
    print("=" * 70)

    # -------------------------------------------------------------------------
    # PREGUNTA 1: Estadística descriptiva de CO (y variables numéricas)
    # -------------------------------------------------------------------------
    print("\n--- PREGUNTA 1: Estadística descriptiva ---")
    df.select("co_ppm", "temperatura_c", "humedad_pct", "velocidad_viento_ms").describe().show()

    stats_co = df.agg(
        F.count("co_ppm").alias("n"),
        F.mean("co_ppm").alias("media"),
        F.stddev("co_ppm").alias("desv_std"),
        F.expr("percentile_approx(co_ppm, 0.5)").alias("mediana"),
        F.min("co_ppm").alias("minimo"),
        F.max("co_ppm").alias("maximo"),
    ).collect()[0]

    print(
        f"Respuesta P1 -> La concentración promedio de CO es {stats_co['media']:.3f} ppm "
        f"(mediana: {stats_co['mediana']:.3f} ppm), con una desviación estándar de {stats_co['desv_std']:.3f} ppm "
        f"y un rango entre {stats_co['minimo']:.3f} y {stats_co['maximo']:.3f} ppm (n = {stats_co['n']} registros válidos)."
    )

    # Gráfico 1: Histograma de distribución de CO (agregado en Spark)
    hist_df = (
        df.withColumn("bin_co", F.round(F.col("co_ppm"), 1))
        .groupBy("bin_co")
        .count()
        .orderBy("bin_co")
        .toPandas()
    )

    plt.figure(figsize=(8, 4.8))
    plt.bar(hist_df["bin_co"], hist_df["count"], width=0.08, color="#2b5c8f", edgecolor="black", alpha=0.85)
    plt.axvline(stats_co["media"], color="#d9534f", linestyle="--", linewidth=1.8, label=f"Media: {stats_co['media']:.3f} ppm")
    plt.axvline(stats_co["mediana"], color="#f0ad4e", linestyle="-.", linewidth=1.8, label=f"Mediana: {stats_co['mediana']:.3f} ppm")
    plt.title("Distribución de Concentración de Monóxido de Carbono (CO)", fontsize=12, fontweight="bold")
    plt.xlabel("Concentración de CO (ppm)")
    plt.ylabel("Frecuencia (cantidad de lecturas)")
    plt.legend()
    plt.grid(axis="y", linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(os.path.join(carpeta_graficos, "pregunta1.png"), dpi=200)
    plt.close()

    # -------------------------------------------------------------------------
    # PREGUNTA 2: Agrupación por categorías (rango_temp x rango_humedad)
    # -------------------------------------------------------------------------
    print("\n--- PREGUNTA 2: Promedio y conteo de CO por Temperatura y Humedad ---")
    tabla_cat = (
        df.groupBy("rango_temp", "rango_humedad")
        .agg(
            F.round(F.avg("co_ppm"), 3).alias("co_promedio_ppm"),
            F.count("*").alias("conteo_eventos"),
        )
        .orderBy("rango_temp", "rango_humedad")
    )
    tabla_cat.show(truncate=False)

    # Pivot en Spark tal como indica docs/contrato_evento.md
    pivot_avg = df.groupBy("rango_temp").pivot("rango_humedad").avg("co_ppm").toPandas()
    pivot_cnt = df.groupBy("rango_temp").pivot("rango_humedad").count().toPandas()

    orden_temp = ["Frío (< 10 °C)", "Templado (10–20 °C)", "Cálido (> 20 °C)"]
    orden_hum = ["Seca (< 40%)", "Moderada (40–70%)", "Húmeda (> 70%)"]

    pivot_avg = pivot_avg.set_index("rango_temp").reindex(index=orden_temp, columns=orden_hum)
    pivot_cnt = pivot_cnt.set_index("rango_temp").reindex(index=orden_temp, columns=orden_hum)

    max_row = tabla_cat.orderBy(F.desc("co_promedio_ppm")).collect()[0]
    print(
        f"Respuesta P2 -> La mayor concentración promedio se registra en condiciones "
        f"'{max_row['rango_temp']}' + '{max_row['rango_humedad']}' con {max_row['co_promedio_ppm']:.3f} ppm "
        f"({max_row['conteo_eventos']} eventos). El frío y la alta humedad reducen la dispersión atmosférica."
    )

    # Gráfico 2: Mapa de calor (Heatmap) con promedio y conteo por celda
    plt.figure(figsize=(8, 5))
    # Celdas sin datos quedan enmascaradas y se pintan en gris (no como CO bajo)
    matriz = np.ma.masked_invalid(pivot_avg.values.astype(float))
    cmap = plt.get_cmap("YlOrRd").copy()
    cmap.set_bad("#d9d9d9")
    im = plt.imshow(matriz, cmap=cmap, aspect="auto")
    plt.colorbar(im, label="CO Promedio (ppm)")

    plt.xticks(range(len(orden_hum)), orden_hum)
    plt.yticks(range(len(orden_temp)), orden_temp)

    for i in range(len(orden_temp)):
        for j in range(len(orden_hum)):
            val = pivot_avg.iloc[i, j]
            cnt = pivot_cnt.iloc[i, j]
            if val is not None and cnt is not None and cnt > 0:
                texto = f"{val:.3f} ppm\n(n={int(cnt)})"
            else:
                texto = "Sin datos\n(n=0)"
            plt.text(j, i, texto, ha="center", va="center", color="black", fontsize=9.5, fontweight="bold")

    plt.title("Concentración Promedio de CO según Temperatura y Humedad", fontsize=12, fontweight="bold")
    plt.xlabel("Categoría de Humedad Relativa")
    plt.ylabel("Categoría de Temperatura")
    plt.tight_layout()
    plt.savefig(os.path.join(carpeta_graficos, "pregunta2.png"), dpi=200)
    plt.close()

    # -------------------------------------------------------------------------
    # PREGUNTA 3: Evolución temporal por hora del día
    # -------------------------------------------------------------------------
    print("\n--- PREGUNTA 3: Evolución temporal por hora del día ---")
    tabla_hora = (
        df.groupBy("hora")
        .agg(
            F.round(F.avg("co_ppm"), 3).alias("co_promedio_ppm"),
            F.count("*").alias("conteo"),
        )
        .orderBy("hora")
    )
    tabla_hora.show(24, truncate=False)

    hora_pd = tabla_hora.toPandas()
    hora_max = hora_pd.loc[hora_pd["co_promedio_ppm"].idxmax()]
    hora_min = hora_pd.loc[hora_pd["co_promedio_ppm"].idxmin()]
    noche = hora_pd[hora_pd["hora"].between(19, 22)]

    texto_noche = ""
    if not noche.empty:
        pico_noche = noche.loc[noche["co_promedio_ppm"].idxmax()]
        texto_noche = (
            f" En la hora punta nocturna (19–22 h) el pico se da a las "
            f"{int(pico_noche['hora']):02d}:00 hrs con {pico_noche['co_promedio_ppm']:.3f} ppm."
        )

    print(
        f"Respuesta P3 -> El CO promedio alcanza su máximo a las {int(hora_max['hora']):02d}:00 hrs "
        f"({hora_max['co_promedio_ppm']:.3f} ppm) y su mínimo a las {int(hora_min['hora']):02d}:00 hrs "
        f"({hora_min['co_promedio_ppm']:.3f} ppm).{texto_noche} "
        f"Los valores más altos coinciden con las horas punta de tráfico y las horas más frías; "
        f"los más bajos, con la tarde, de mayor temperatura y ventilación."
    )

    # Gráfico 3: Serie temporal horaria
    plt.figure(figsize=(9, 4.8))
    plt.plot(hora_pd["hora"], hora_pd["co_promedio_ppm"], marker="o", color="#1f77b4", linewidth=2.2)
    plt.axvspan(7, 9, color="#ffbb78", alpha=0.3, label="Hora punta mañana (07–09 h)")
    plt.axvspan(19, 22, color="#ff7f0e", alpha=0.25, label="Hora punta noche (19–22 h)")
    plt.axvspan(3, 5, color="#98df8a", alpha=0.3, label="Madrugada (03–05 h)")
    plt.xticks(range(0, 24))
    plt.title("Evolución Horaria Promedio de Monóxido de Carbono (CO)", fontsize=12, fontweight="bold")
    plt.xlabel("Hora del día (0–23 h)")
    plt.ylabel("Concentración promedio de CO (ppm)")
    plt.legend(loc="upper left", fontsize=8.5)
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(os.path.join(carpeta_graficos, "pregunta3.png"), dpi=200)
    plt.close()

    # -------------------------------------------------------------------------
    # CONCLUSIÓN METODOLÓGICA (Regla obligatoria del contrato)
    # -------------------------------------------------------------------------
    print("\n--- CONCLUSIÓN METODOLÓGICA ---")
    print(
        "Nota: Los resultados reflejan el comportamiento de un dataset sintético calibrado con rangos reales "
        "de la estación Las Condes, donde los picos en frío, alta humedad y horas punta responden tanto a "
        "patrones físicos de dispersión como a las reglas multiplicativas definidas en el productor."
    )
    print("=" * 70 + "\n")

    spark.stop()


if __name__ == "__main__":
    main()