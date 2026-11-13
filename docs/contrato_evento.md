<!--
CONTRATO VALIDADO (13-11-2026). Todos los scripts deben respetarlo.
NO cambiar nombres ni tipos de campos por cuenta propia: cualquier opinión o
propuesta de cambio se comenta primero.
-->

# Contrato del evento — Solemne 2 Big Data

## Caso

**Contexto y problema:** La estación de monitoreo de calidad del aire de Las Condes
(Santiago) registra cada hora la concentración de monóxido de carbono (CO) junto con
variables meteorológicas. El CO proviene principalmente del tráfico vehicular, y su
acumulación depende de la hora del día y de las condiciones atmosféricas (frío, humedad,
viento). Se busca entender cuándo y bajo qué condiciones se registran las mayores
concentraciones.

**Objetivo general:** Implementar un flujo Python → Kafka → PySpark que simule la
transmisión de lecturas de la estación y analice cómo se comporta la concentración de CO
según las condiciones de temperatura, humedad y la hora del día.

**Origen de los datos:** Datos **sintéticos** generados por el productor. Los rangos y
proporciones se basaron en los registros reales de la estación Las Condes (CSV de CO,
temperatura, humedad, velocidad y dirección del viento). Los resultados responden en parte
a las reglas de generación definidas abajo.

## Preguntas de análisis

1. **Descriptiva:** ¿Cuál es la concentración promedio y la variabilidad de CO registrada por la estación?
2. **Comparación entre categorías:** ¿Cómo varía la concentración de CO según las condiciones de temperatura y humedad?
3. **Evolución temporal:** ¿Cómo varía el CO a lo largo del día?

## Campos del evento

| Campo | Tipo | Descripción | Valores / rango |
|---|---|---|---|
| `id_evento` | string | Identificador único | `"EV-000001"` … correlativo |
| `timestamp` | string ISO 8601 | Fecha y hora de la lectura | entre 2025-01-01 y 2025-12-31 |
| `direccion_viento` | string (categórica) | Dirección de donde viene el viento | `N, NE, E, SE, S, SO, O, NO` |
| `co_ppm` | float | Monóxido de carbono (ppm) | 0,05 – 3,0 (promedio ~0,5) · **puede ser `null`** |
| `temperatura_c` | float | Temperatura (°C) | ~0 – 35 |
| `humedad_pct` | float | Humedad relativa (%) | 10 – 100 |
| `velocidad_viento_ms` | float | Velocidad del viento (m/s) | 0 – 6 |

Ejemplo de evento:
```json
{"id_evento": "EV-000123", "timestamp": "2025-07-14T20:00:00", "direccion_viento": "SO", "co_ppm": 0.82, "temperatura_c": 7.4, "humedad_pct": 81.2, "velocidad_viento_ms": 0.6}
```

## Reglas de generación (para el productor)

- **timestamp:** fecha y hora aleatorias dentro de 2025, así quedan representadas todas las estaciones del año y todas las horas.
- **direccion_viento:** sorteo con las frecuencias reales de la estación (CSV de dirección agrupado en 8 sectores):

  | N | NE | E | SE | S | SO | O | NO |
  |---|---|---|---|---|---|---|---|
  | 1,7% | 19,5% | 22,6% | 3,0% | 6,4% | 29,9% | 15,0% | 1,9% |

- **temperatura_c:** base según el mes (verano ~22 °C, invierno ~9 °C) + ciclo diario (mínima ~6 h, máxima ~15 h, ±6 °C) + ruido.
- **humedad_pct:** inversa a la temperatura (más frío → más húmedo) + ruido, acotada a 10–100.
- **velocidad_viento_ms:** mayor en la tarde (~2 m/s) y menor de noche (~0,8 m/s) + ruido, nunca negativa.

## Patrón intencional

El CO parte de una base de ~0,45 ppm y se multiplica por factores:

| Condición | Factor | Justificación (datos reales) |
|---|---|---|
| Hora punta mañana (7–9 h) | ×1,3 | Tráfico. Real: 0,53 ppm a las 8 h |
| Hora punta noche (19–22 h) | ×1,4 | Tráfico + menos ventilación. Real: 0,59 ppm a las 21 h |
| Madrugada (3–5 h) | ×0,8 | Poco tráfico. Real: 0,37 ppm a las 4 h |
| Frío (< 10 °C) | ×1,5 | Inversión térmica en invierno |
| Humedad alta (> 70%) | ×1,3 | Atmósfera estable, menor dispersión |
| Viento débil (< 1 m/s) | ×1,2 | Menor dispersión |

Resultado esperado: el CO más alto se da con **frío + humedad alta** (mapa de calor, pregunta 2)
y en la **noche** (pregunta 3).

## Problema de calidad controlado

- **Qué:** valores **nulos** en `co_ppm`, simulando fallas del sensor.
- **Cuánto:** ~3% de los eventos, elegidos al azar.
- **Cómo se introduce:** al generar cada evento, con probabilidad 0,03 se asigna `co_ppm = None` (en el JSON queda `null`). El productor informa cuántos nulos generó.
- **Respaldo real:** los CSV de la estación también tienen horas sin registro y valores fuera de rango (por ejemplo, temperaturas de -40 °C como código de error).
- **Tratamiento sugerido (lo decide y justifica Adrián en `preparacion.py`):** eliminar los registros con `co_ppm` nulo, porque es la variable principal del análisis e imputarla alteraría los promedios. Se muestran los conteos antes y después.

## Categorías creadas en Spark (para la pregunta 2)

No vienen en el evento; se derivan en `preparacion.py`:

| Columna | Categorías |
|---|---|
| `rango_temp` | Frío (< 10 °C) · Templado (10–20 °C) · Cálido (> 20 °C) |
| `rango_humedad` | Seca (< 40%) · Moderada (40–70%) · Húmeda (> 70%) |

Análisis: `groupBy("rango_temp").pivot("rango_humedad").avg("co_ppm")` → mapa de calor.
Mostrar también el conteo de eventos por celda, porque algunas combinaciones (cálido + húmedo) son poco frecuentes.

## Conexión

- Servidor Kafka (bootstrap servers): `localhost:9092`
- Tópico: `estacion-lascondes`
