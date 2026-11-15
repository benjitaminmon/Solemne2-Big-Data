import argparse
import json
import os
import random
import sys
import time
import warnings
from datetime import datetime, timedelta

# Suprimir advertencias internas de compatibilidad de serializadores en kafka-python
warnings.filterwarnings("ignore", category=DeprecationWarning)

# =============================================================================
# CONSTANTES Y CONFIGURACIÓN POR DEFECTO
# =============================================================================

BOOTSTRAP_SERVERS_DEFAULT = "localhost:9092"
TOPIC_DEFAULT = "estacion-lascondes"
CANTIDAD_DEFAULT = 5000
PROB_ANOMALIA = 0.03  # 3% de los eventos con co_ppm = None (anomalía controlada)

# Frecuencias reales de dirección del viento de la estación Las Condes (docs/contrato_evento.md)
DIRECCIONES_VIENTO = ["N", "NE", "E", "SE", "S", "SO", "O", "NO"]
PESOS_DIRECCION = [1.7, 19.5, 22.6, 3.0, 6.4, 29.9, 15.0, 1.9]

# Temperatura base promedio mensual (°C) en Santiago
TEMP_BASE_MES = {
    1: 22.0,   # Enero (verano)
    2: 21.5,   # Febrero
    3: 19.0,   # Marzo (otoño)
    4: 15.5,   # Abril
    5: 12.0,   # Mayo
    6: 9.0,    # Junio (invierno)
    7: 8.5,    # Julio (invierno)
    8: 10.0,   # Agosto
    9: 13.0,   # Septiembre (primavera)
    10: 16.0,  # Octubre
    11: 19.0,  # Noviembre
    12: 21.5   # Diciembre (verano)
}


# =============================================================================
# GENERACIÓN DE EVENTOS
# =============================================================================

def generar_evento(id_num: int) -> dict:
    id_evento = f"EV-{id_num:06d}" # "06" hace cumplir que el ID tenga 6 dígitos

    # Timestamp aleatorio
    dia_del_anio = random.randint(0, 364)
    hora = random.randint(0, 23)

    # datetime(Año, Mes, Día, Hora, Minuto, Segundo)
    dt = datetime(2025, 1, 1, 0, 0, 0) + timedelta(days=dia_del_anio, hours=hora)
    timestamp_str = dt.strftime("%Y-%m-%dT%H:%M:%S")
    mes = dt.month

    direccion_viento = random.choices(DIRECCIONES_VIENTO, weights=PESOS_DIRECCION, k=1)[0]

    # Temperatura (°C): base estacional mensual + ciclo diario (mínima ~6 h, máxima ~15 h, ±6 °C) + ruido
    if 6 <= hora <= 15:
        ciclo_diario = -6.0 + 12.0 * ((hora - 6) / 9.0)   # Calentamiento diurno (6:00 a 15:00)
    else:
        horas_noche = (hora - 15) % 24                    # Horas transcurridas desde las 15:00
        ciclo_diario = 6.0 - 12.0 * (horas_noche / 15.0)  # Enfriamiento nocturno (15:00 a 6:00)

    temperatura_c = round(max(0.0, min(35.0, TEMP_BASE_MES[mes] + ciclo_diario + random.uniform(-1.5, 1.5))), 1)

    # Humedad (%): relación inversa con temperatura (más frío -> más húmedo) + ruido, acotada a [10, 100]
    # A 0 °C la base es ~90%; por cada 1 °C que sube la temperatura, la humedad baja ~2%
    humedad_calc = 90.0 - (temperatura_c * 2.0) + random.uniform(-5.0, 5.0)
    humedad_pct = round(max(10.0, min(100.0, humedad_calc)), 1)

    # Velocidad del viento (m/s): mayor en la tarde (~2.0 m/s), menor de noche (~0.8 m/s)
    if 13 <= hora <= 19:
        velocidad_viento_ms = round(random.uniform(1.5, 2.5), 1)
    else:
        velocidad_viento_ms = round(random.uniform(0.5, 1.2), 1)

    # Concentración de CO (ppm) con PATRÓN INTENCIONAL
    co_base = 0.45
    factor = 1.0

    # Factores por hora (tráfico y ventilación atmosférica)
    if 7 <= hora <= 9:
        factor *= 1.3  # Punta mañana
    elif 19 <= hora <= 22:
        factor *= 1.4  # Punta noche
    elif 3 <= hora <= 5:
        factor *= 0.8  # Madrugada con poco tráfico

    # Factores meteorológicos (dispersión atmosférica)
    if temperatura_c < 10.0:
        factor *= 1.5  # Frío / inversión térmica

    if humedad_pct > 70.0:
        factor *= 1.3  # Humedad alta / estabilidad
        
    if velocidad_viento_ms < 1.0:
        factor *= 1.2  # Viento débil / menor dispersión

    # Ruido ambiental controlado
    co_calc = round(co_base * factor * random.uniform(0.92, 1.08), 2)
    co_ppm = max(0.05, min(3.0, co_calc))

    # Anomalía controlada: ~3% de lecturas nulas simulando sensor desconectado/averiado
    if random.random() < PROB_ANOMALIA:
        co_ppm = None

    # Estructura final del evento (coincide con docs/contrato_evento.md)
    return {
        "id_evento": id_evento,
        "timestamp": timestamp_str,
        "direccion_viento": direccion_viento,
        "co_ppm": co_ppm,
        "temperatura_c": temperatura_c,
        "humedad_pct": humedad_pct,
        "velocidad_viento_ms": velocidad_viento_ms
    }


# =====================
# Generar un archivo
# =====================
def enviar_a_archivo(eventos: list, ruta_archivo: str) -> tuple[int, int]:
    directorio = os.path.dirname(ruta_archivo)
    if directorio:
        os.makedirs(directorio, exist_ok=True)

    escritos = 0
    errores = 0

    with open(ruta_archivo, "w", encoding="utf-8") as f:
        for ev in eventos:
            try:
                linea = json.dumps(ev, ensure_ascii=False)
                f.write(linea + "\n")
                escritos += 1
            except Exception as e:
                errores += 1
                print(f"[ERROR] No se pudo serializar/escribir evento {ev.get('id_evento')}: {e}", file=sys.stderr)

    return escritos, errores

# =====================
# Enviar a Kafka
# =====================
def enviar_a_kafka(eventos: list, bootstrap_servers: str, topico: str, delay: float = 0.0) -> tuple[int, int]:
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("[ERROR CRÍTICO] La librería 'kafka-python' no está instalada.", file=sys.stderr)
        print("Instálala con: pip install kafka-python>=2.0.3", file=sys.stderr)
        return 0, len(eventos)

    print(f"[*] Conectando a Kafka en {bootstrap_servers}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            retries=2,
            request_timeout_ms=5000,
            max_block_ms=5000
        )
    except Exception as e:
        print(f"[ERROR CRÍTICO] No fue posible conectar con Kafka en {bootstrap_servers}: {e}", file=sys.stderr)
        print("\nSugerencias:", file=sys.stderr)
        print("  1. Verifica que el contenedor esté corriendo con: docker ps", file=sys.stderr)
        print("  2. Inicia Kafka con: docker compose -f kafka/docker_compose.yml up -d", file=sys.stderr)
        print("  3. Para generar datos sin Kafka (modo prueba), usa la opción --solo-archivo:", file=sys.stderr)
        print("     python src/productor.py --cantidad 500 --solo-archivo data/eventos_prueba.jsonl\n", file=sys.stderr)
        return 0, len(eventos)

    print(f"[*] Conexión exitosa. Publicando en el tópico '{topico}'...")
    enviados = 0
    errores = 0

    def on_send_error(excp):
        nonlocal errores
        errores += 1
        print(f"[ERROR] Falla al entregar mensaje a Kafka: {excp}", file=sys.stderr)

    total = len(eventos)
    paso_progreso = max(1, total // 10)

    for idx, ev in enumerate(eventos, start=1):
        try:
            future = producer.send(topico, value=ev)
            future.add_errback(on_send_error)
            enviados += 1

            if delay > 0:
                time.sleep(delay)

            # Informar progreso periódico si la cantidad es grande
            if idx % paso_progreso == 0 or idx == total:
                print(f"    -> Progreso: {idx}/{total} eventos procesados ({(idx/total)*100:.1f}%)")

        except Exception as e:
            errores += 1
            print(f"[ERROR] Error al encolar evento {ev.get('id_evento')}: {e}", file=sys.stderr)

    print("[*] Vaciando buffer de Kafka (flush)...")
    try:
        producer.flush(timeout=10)
        producer.close()
    except Exception as e:
        print(f"[ADVERTENCIA] Error durante el flush o cierre del productor: {e}", file=sys.stderr)

    return enviados - errores, errores


# =============================================================================
# CLI Y FUNCIÓN PRINCIPAL
# =============================================================================

# Le indica al archivo como comportarse ante distintos comandos
def parse_args():
    parser = argparse.ArgumentParser(
        description="Productor de eventos meteorológicos y calidad del aire para Kafka / JSONL.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-n", "--cantidad",
        type=int,
        default=CANTIDAD_DEFAULT,
        help="Cantidad de eventos a generar (ej: 5000 para análisis completo, 150 para video demo)."
    )
    parser.add_argument(
        "-o", "--solo-archivo",
        type=str,
        default=None,
        help="Ruta de archivo JSON Lines para guardar eventos directamente sin Kafka (ej: data/eventos_prueba.jsonl)."
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=BOOTSTRAP_SERVERS_DEFAULT,
        help="Dirección del broker Kafka."
    )
    parser.add_argument(
        "--topico",
        type=str,
        default=TOPIC_DEFAULT,
        help="Nombre del tópico de Kafka."
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Tiempo de espera en segundos entre eventos enviados a Kafka (opcional, útil para la demo en vivo)."
    )
    parser.add_argument(
        "--semilla",
        type=int,
        default=None,
        help="Semilla para el generador pseudoaleatorio (opcional, para reproducibilidad)."
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.semilla is not None:
        random.seed(args.semilla)

    inicio = time.time()

    print("=" * 70)
    print("  PRODUCTOR DE EVENTOS — ESTACIÓN LAS CONDES (SOLEMNE 2 BIG DATA)")
    print("=" * 70)

    print(f"Cantidad solicitada : {args.cantidad:,} eventos")
    if args.solo_archivo:
        print(f"Modo de ejecución   : SOLO ARCHIVO (sin Kafka)")
        print(f"Ruta de salida      : {args.solo_archivo}")
    else:
        print(f"Modo de ejecución   : KAFKA")
        print(f"Broker              : {args.bootstrap_servers}")
        print(f"Tópico              : {args.topico}")
        if args.delay > 0:
            print(f"Delay entre envíos  : {args.delay} s")
    print("-" * 70)

    # 1. Generación de eventos en memoria
    print(f"[*] Generando {args.cantidad:,} eventos...")
    eventos = [generar_evento(i) for i in range(1, args.cantidad + 1)]

    # Conteo de anomalías
    nulos = sum(1 for e in eventos if e["co_ppm"] is None)
    pct_nulos = (nulos / args.cantidad) * 100

    # 2. Publicación o guardado
    if args.solo_archivo:
        exitosos, errores = enviar_a_archivo(eventos, args.solo_archivo)
    else:
        exitosos, errores = enviar_a_kafka(
            eventos,
            bootstrap_servers=args.bootstrap_servers,
            topico=args.topico,
            delay=args.delay
        )

    duracion = time.time() - inicio

    # 3. Muestra de eventos generados (primeros 2 y último)
    print("\n" + "-" * 70)
    print("MUESTRA DE EVENTOS GENERADOS:")
    print("-" * 70)
    muestra_indices = [0, 1] if len(eventos) >= 2 else [0]
    if len(eventos) > 2:
        muestra_indices.append(len(eventos) - 1)

    for idx in muestra_indices:
        print(f"  [{idx + 1}] {json.dumps(eventos[idx], ensure_ascii=False)}")

    # 4. Informe final detallado
    print("\n" + "=" * 70)
    print("RESUMEN DE EJECUCIÓN:")
    print("=" * 70)
    print(f"  • Total eventos generados   : {args.cantidad:,}")
    print(f"  • Eventos emitidos con éxito: {exitosos:,}")
    print(f"  • Anomalías (co_ppm = null) : {nulos:,} ({pct_nulos:.2f}% de los datos — objetivo ~3%)")
    print(f"  • Errores detectados        : {errores:,}")
    print(f"  • Tiempo total transcurrido : {duracion:.2f} segundos")
    if exitosos > 0 and duracion > 0:
        print(f"  • Rendimiento               : {exitosos / duracion:.1f} eventos/segundo")
    print("=" * 70)

    if errores > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
