<!--
TAREA — Responsable: Benjamín Lagos, con todo el equipo  (BORRAR ESTE BLOQUE AL TERMINAR)
PRIORIDAD MÁXIMA: definir HOY. Todos los scripts dependen de este documento.
Una vez acordado, NO cambiar nombres ni tipos de campos sin avisar al equipo.
-->

# Contrato del evento

## Caso
Contexto y problema (breve):

Objetivo general:

## Preguntas de análisis
1. Descriptiva:
2. Comparación entre categorías:
3. Evolución temporal:

## Campos del evento

| Campo | Tipo | Descripción | Valores / rango |
|---|---|---|---|
| id_evento | string | Identificador único | |
| timestamp | string ISO 8601 | Marca temporal | ej: 2026-11-13T14:05:00 |
| (categórica) | string | | lista de categorías |
| (numérica) | float | | rango normal |
| (adicional) | | | |

Ejemplo de evento:
```json
{}
```

## Patrón intencional
(ej: la categoría X tiene valores más altos; los valores suben en horario nocturno)

## Problema de calidad controlado
Qué anomalía, en qué campo, en qué porcentaje y cómo se introduce en el productor:
