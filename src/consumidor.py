import argparse
import json
import os
import sys
import time
import warnings

# Suprimir advertencias internas de compatibilidad en kafka-python
warnings.filterwarnings("ignore", category=DeprecationWarning)
# Constantes y configuracion por defecto.

BOOTSTRAP_SERVERS_DEFAULT = "localhost:9092"
TOPIC_DEFAULT = "estacion-lascondes"
SALIDA_DEFAULT = "data/eventos.jsonl"
GROUP_ID_DEFAULT = "grupo-estacion-lascondes"
TIMEOUT_DEFAULT = 8.0
MUESTRA_DEFAULT = 5

CAMPOS_CONTRATO = {
    "id_evento",
    "timestamp",
    "direccion_viento",
    "co_ppm",
    "temperatura_c",
    "humedad_pct",
    "velocidad_viento_ms",
}
# Cli y argumentos 

def parse_args():
    parser = argparse.ArgumentParser(
        description="Consumidor Kafka de eventos meteorológicos y calidad del aire.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=BOOTSTRAP_SERVERS_DEFAULT,
        help="Dirección del broker de Kafka.",
    )
    parser.add_argument(
        "--topico",
        type=str,
        default=TOPIC_DEFAULT,
        help="Nombre del tópico de Kafka al cual suscribirse.",
    )
    parser.add_argument(
        "-o", "--salida",
        type=str,
        default=SALIDA_DEFAULT,
        help="Ruta del archivo JSON Lines de salida (ej: data/eventos.jsonl o data/muestra_demo.jsonl).",
    )
    parser.add_argument(
        "-n", "--max-eventos",
        type=int,
        default=None,
        help="Cantidad máxima opcional de eventos válidos a guardar antes de finalizar.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=TIMEOUT_DEFAULT,
        help="Segundos de espera sin mensajes nuevos para finalizar automáticamente.",
    )
    parser.add_argument(
        "--muestra",
        type=int,
        default=MUESTRA_DEFAULT,
        help="Cantidad de mensajes de ejemplo a mostrar en consola.",
    )
    parser.add_argument(
        "--group-id",
        type=str,
        default=GROUP_ID_DEFAULT,
        help="Identificador del grupo de consumidores de Kafka.",
    )
    return parser.parse_args()

# Lógica principal del consumidor
def main():
    args = parse_args()

    try:
        from kafka import KafkaConsumer
        from kafka.errors import KafkaError
    except ImportError:
        print("[ERROR CRÍTICO] La librería 'kafka-python' no está instalada.", file=sys.stderr)
        print("Instálala con: pip install kafka-python>=2.0.3", file=sys.stderr)
        sys.exit(1)

    # Crear carpeta de destino si no existe
    directorio = os.path.dirname(args.salida)
    if directorio:
        os.makedirs(directorio, exist_ok=True)

    print("=" * 70)
    print("  CONSUMIDOR DE EVENTOS — ESTACIÓN LAS CONDES")
    print("=" * 70)
    print(f"Broker              : {args.bootstrap_servers}")
    print(f"Tópico              : {args.topico}")
    print(f"Grupo consumidor    : {args.group_id}")
    print(f"Archivo de salida   : {args.salida}")
    print(f"Cierre por silencio : {args.timeout} segundos sin nuevos mensajes")
    if args.max_eventos is not None:
        print(f"Límite de eventos   : {args.max_eventos:,}")
    print("-" * 70)

    # 1. Inicio de recepción: conexión y suscripción al tópico
    print(f"[*] Suscribiéndose al tópico '{args.topico}'...")
    try:
        consumer = KafkaConsumer(
            args.topico,
            bootstrap_servers=args.bootstrap_servers,
            group_id=args.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=int(args.timeout * 1000),
        )
    except KafkaError as e:
        print(f"[ERROR CRÍTICO] No fue posible conectar con Kafka: {e}", file=sys.stderr)
        print("Verifica que Docker esté corriendo: docker compose -f kafka/docker_compose.yml up -d", file=sys.stderr)
        sys.exit(1)

    inicio = time.time()
    mensajes_leidos = 0
    eventos_guardados = 0
    nulos_detectados = 0
    errores_interpretacion = 0

    print("[*] Esperando eventos del tópico...\n")

    try:
        with open(args.salida, mode="w", encoding="utf-8") as archivo_jsonl:
            for mensaje in consumer:
                mensajes_leidos += 1
                try:
                    # Decodificación de bytes y deserialización JSON
                    texto_crudo = mensaje.value.decode("utf-8")
                    evento = json.loads(texto_crudo)

                    # Validar que sea un objeto JSON y cumpla los campos del contrato
                    if not isinstance(evento, dict):
                        raise ValueError("El mensaje JSON no es un objeto válido.")

                    faltantes = CAMPOS_CONTRATO - set(evento.keys())
                    if faltantes:
                        raise ValueError(f"Faltan campos del contrato: {sorted(faltantes)}")

                    # Contabilizar lecturas con co_ppm nulo (anomalía esperada ~3%)
                    if evento.get("co_ppm") is None:
                        nulos_detectados += 1

                    # Guardar como una línea JSON independiente (JSON Lines)
                    archivo_jsonl.write(json.dumps(evento, ensure_ascii=False) + "\n")
                    eventos_guardados += 1

                    # Mostrar pequeña muestra de mensajes recibidos en consola
                    if eventos_guardados <= args.muestra:
                        print(
                            f"  [Muestra #{eventos_guardados} | Partición {mensaje.partition} | "
                            f"Offset {mensaje.offset}] {json.dumps(evento, ensure_ascii=False)}"
                        )

                    # Cierre controlado si se alcanza el límite solicitado
                    if args.max_eventos is not None and eventos_guardados >= args.max_eventos:
                        print(f"\n[*] Se alcanzó el límite solicitado de {args.max_eventos:,} eventos.")
                        break

                except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as err:
                    errores_interpretacion += 1
                    print(
                        f"[ADVERTENCIA] Mensaje no interpretable descartado "
                        f"(partición={mensaje.partition}, offset={mensaje.offset}): {err}",
                        file=sys.stderr,
                    )

    except KeyboardInterrupt:
        print("\n[*] Recepción detenida manualmente por el usuario (Ctrl+C).")
    finally:
        # 2. Finalización controlada: cerrar conexión y liberar recursos
        consumer.close()

    duracion = time.time() - inicio
    pct_nulos = (nulos_detectados / eventos_guardados * 100) if eventos_guardados > 0 else 0.0

    print("\n" + "=" * 70)
    print("Resumend e ejecución del consumidor:")
    print("=" * 70)
    print(f"  • Mensajes leídos del tópico      : {mensajes_leidos:,}")
    print(f"  • Eventos válidos guardados       : {eventos_guardados:,}")
    print(f"  • Eventos con co_ppm = null       : {nulos_detectados:,} ({pct_nulos:.2f}%)")
    print(f"  • Errores de interpretación       : {errores_interpretacion:,}")
    print(f"  • Archivo JSON Lines generado     : {args.salida}")
    print(f"  • Tiempo total transcurrido       : {duracion:.2f} segundos")
    print("=" * 70)

    if errores_interpretacion > 0 and eventos_guardados == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
