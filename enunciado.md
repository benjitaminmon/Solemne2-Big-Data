# Solemne 02: Big Data - Generación, Ingesta y Análisis de Eventos con Python, Kafka y Spark

## 1. Resultados de Aprendizaje

* Utiliza los conceptos de procesamiento en tiempo real, aprovechando plataformas de data streaming y aplicando técnicas de análisis predictivo, identificando patrones y tendencias dentro de conjuntos de datos extensos. Además, demuestra una comprensión al abordar los desafíos éticos inherentes al análisis de datos, destacando su compromiso con la seguridad y protección de la información, promoviendo prácticas responsables y éticas en el manejo de datos en entornos de Big Data.
* Propone propuestas innovadoras y originales fundamentadas en conocimientos de Big Data, incorporando la integración de tecnologías emergentes en el diseño de soluciones para enfrentar desafíos específicos en el mundo del Big Data.

---

## 2. Instrucciones y Condiciones de Evaluación

La presente evaluación corresponde a la parte práctica de la prueba **Solemne 02** de la asignatura **Big Data** y se desarrollará mediante un **trabajo en equipo**.

### Consideraciones Generales
* **Equipos de trabajo:** Se debe trabajar en equipos de **3 a 6 estudiantes**. Estos deben ser informados al docente a más tardar el **12-11-2026**. Todo estudiante que no logre conformar equipo debe informar de esto al docente en esa misma fecha para que sea asignado a uno. Si no informa y entrega de forma individual sin autorización del docente, **su nota máxima será un 4.0**.
* **Entrega en Canvas:** Cada estudiante debe subir la presentación a Canvas. Todo estudiante que no lo suba o lo entregue por otro medio sufrirá una **penalización de 2 puntos de descuento** sobre la nota obtenida en esta evaluación.
* **Cumplimiento:** El incumplimiento de las condiciones establecidas podrá afectar la calificación final de la Solemne 02.

---

## 3. Descripción y Propósito

Se solicita implementar un flujo básico que permita generar eventos mediante Python, publicarlos en Apache Kafka, recibirlos mediante un consumidor Python y almacenarlos en un archivo. Posteriormente, se deberá cargar ese archivo en un DataFrame de PySpark y realizar un análisis exploratorio que responda tres preguntas relacionadas con un caso seleccionado.

* **Modalidad de evaluación:** Presentación breve, demostración práctica y preguntas individuales (no se solicitará un informe escrito).
* **Enfoque:** Comprender y demostrar la función de cada herramienta, implementar un flujo funcional e interpretar los resultados.

---

## 4. Selección del Caso

Cada equipo deberá seleccionar un **caso sencillo**, distinto de los ejemplos resueltos en clases. Podrá abordar **ventas, sensores, transporte, logística u otro contexto pertinente**.

Deberá definir:
1. Una descripción breve del contexto y del problema.
2. Un objetivo general.
3. Tres preguntas de análisis:
   * Una pregunta descriptiva.
   * Una comparación entre categorías.
   * Una pregunta sobre evolución temporal.
   *(Nota: Las preguntas deberán poder responderse con los atributos generados).*

---

## 5. Arquitectura Requerida

![alt text](image.png)


El consumidor guardará cada evento recibido como un objeto JSON en una línea independiente del archivo. Spark leerá ese archivo una vez finalizada la captura; **no se solicita lectura directa desde Kafka ni procesamiento con Structured Streaming**.

---

## 6. Actividades y Requisitos Técnicos

### 6.1. Investigación breve de Kafka
Cada equipo deberá investigar y explicar:
* Qué función cumple Kafka en su proyecto.
* Qué es un productor.
* Qué es un consumidor.
* Qué es un tópico.
* Qué representa un offset, a nivel conceptual.
*(La explicación deberá relacionarse con la implementación e identificar las fuentes consultadas. No se solicita una investigación extensa ni una exposición sobre administración avanzada de Kafka).*

### 6.2. Productor Python
El productor deberá:
* Generar y publicar **al menos 5.000 eventos**.
* Utilizar formato **JSON**.
* Incluir **al menos cinco atributos**:
  1. Identificador del evento.
  2. Marca temporal.
  3. Una variable categórica.
  4. Una variable numérica.
  5. Un atributo adicional pertinente.
* Permitir modificar la cantidad de eventos mediante una variable o parámetro.
* Informar la cantidad de eventos enviados y los errores detectados, si los hubiera.

> **Nota sobre patrones y calidad:** Los datos deberán incorporar un patrón intencional sencillo (como diferencias de valores entre categorías o variaciones según el horario) y un **tipo de problema de calidad controlado** (como valores nulos o valores fuera de rango). El equipo deberá explicar cómo lo introdujo. No se exige interfaz de configuración, generación avanzada de datos ni múltiples tipos de anomalías.

### 6.3. Consumidor Python
El consumidor deberá:
* Suscribirse al tópico del proyecto.
* Recibir y deserializar los eventos.
* Guardarlos en un archivo **JSON Lines**.
* Mostrar una pequeña muestra de mensajes recibidos.
* Informar la cantidad de eventos guardados.
* Registrar o informar los mensajes que no puedan interpretarse.

> **Nota operativa:** El equipo deberá explicar cómo inicia y finaliza la recepción. No se solicita prueba de reinicio, administración avanzada de offsets ni múltiples consumidores.

### 6.4. Carga y preparación con PySpark
El equipo deberá cargar el archivo recibido en un DataFrame de PySpark y realizar las siguientes acciones:
1. Mostrar el esquema.
2. Contar los registros.
3. Comprobar los tipos de las variables utilizadas en el análisis.
4. Convertir la marca temporal al tipo correspondiente.
5. Identificar y tratar el problema de calidad introducido.
6. Mostrar la cantidad de registros antes y después del tratamiento.

> **Alcance:** Se podrá utilizar la inferencia de esquema, siempre que el equipo revise el resultado y realice las conversiones necesarias. No se exige persistencia en Parquet, configuración de checkpoints ni separación de múltiples capas de almacenamiento.

### 6.5. Análisis exploratorio
El análisis deberá incluir:
* Estadística descriptiva de al menos una variable numérica.
* Una agrupación o comparación por categoría.
* Una agrupación temporal.
* Respuestas explícitas a las tres preguntas iniciales.
* **Tres visualizaciones**, una por pregunta.

> **Herramientas:** Las operaciones principales de preparación y análisis deberán realizarse con PySpark. Se permitirá utilizar Pandas y una biblioteca gráfica para visualizar resultados agregados. Cada gráfico deberá incluir título, ejes identificados e interpretación breve. Las conclusiones deberán reconocer que los datos son sintéticos y que algunos resultados responden a las reglas de generación.

---

## 7. Presentación del Proyecto

Cada equipo deberá entregar una presentación en **PDF o PPTX de un máximo de nueve diapositivas**.

### Distribución sugerida:
* **Diapositiva 1:** Nombre del proyecto, integrantes y distribución de tareas.
* **Diapositiva 2:** Contexto, objetivo y tres preguntas.
* **Diapositiva 3:** Arquitectura y explicación breve de Kafka.
* **Diapositiva 4:** Atributos, patrón generado y problema de calidad.
* **Diapositiva 5:** Funcionamiento del productor, consumidor y carga en Spark.
* **Diapositiva 6:** Preparación de datos y resultados descriptivos.
* **Diapositiva 7:** Tres visualizaciones y respuestas a las preguntas.
* **Diapositiva 8:** Demo de su solución según lo indicado más abajo.
* **Diapositiva 9:** Conclusiones, limitaciones y fuentes.

*(La distribución podrá ajustarse, siempre que se incluyan todos los contenidos. No se solicitará incorporar código completo en las diapositivas).*

**Tiempos:** Para la presentación dispondrán de un **mínimo de 12 minutos y un máximo de 15 minutos**. El docente informará las fechas tras la conformación de los equipos.
> *Nota sobre penalización de tiempo:* Todo minuto por sobre el tiempo máximo o bajo el tiempo mínimo descontará **1 punto** sobre la nota obtenida, con un máximo de **3 puntos de descuento**.

---

## 8. Demostración Práctica

Durante la demostración, el equipo deberá:
1. Mostrar el tópico y el entorno preparado.
2. Ejecutar el consumidor y el productor.
3. Evidenciar el envío y la recepción de una muestra de eventos.
4. Mostrar el archivo generado por el consumidor.
5. Cargarlo con PySpark.
6. Mostrar el esquema, el conteo y el tratamiento del problema de calidad.
7. Ejecutar una operación representativa del análisis.
8. Explicar los resultados y las visualizaciones.

> **Consideraciones de la demo:** La demostración podrá realizarse con una muestra de entre **100 y 200 eventos**. El archivo completo de al menos **5.000 eventos** deberá entregarse y estar disponible para revisión. La muestra de demostración deberá guardarse separadamente para no alterar los resultados del conjunto completo. Puede presentar un video o ejecutar en vivo usando un PC propio.

---

## 9. Entregables

Cada equipo deberá entregar:
1. Presentación de máximo nueve diapositivas.
2. Código del productor Python.
3. Código del consumidor Python.
4. Script de PySpark con preparación y análisis.
5. Archivo JSON Lines con al menos 5.000 eventos recibidos.
6. Tres visualizaciones (incorporadas en la presentación o disponibles como archivos).
7. **Archivo README**, el cual deberá indicar únicamente:
   * Dependencias y versiones utilizadas.
   * Configuración básica de conexión y nombre del tópico.
   * Orden y comandos de ejecución.
   * Ubicación del archivo de datos.

---

## 10. Rúbrica de Evaluación

| Criterio | Sobresaliente (8 a 10 puntos) | Adecuado (5 a 7 puntos) | Básico (3 a 6 puntos) | Insuficiente (0 a 2 puntos) |
| :--- | :--- | :--- | :--- | :--- |
| **Caso, diseño de datos y comprensión de Kafka** | Define contexto, objetivo y tres preguntas coherentes; cumple los atributos, explica patrón y anomalía, y relaciona los conceptos de Kafka con su flujo. | Presenta los elementos requeridos, con una justificación limitada en alguna pregunta, regla de generación o concepto de Kafka. | Omite preguntas o atributos, o explica parcialmente la relación entre el caso y los componentes. | No define un caso analizable ni distingue las funciones del productor, Kafka y consumidor. |
| **Productor y consumidor Python** | Publica y recibe al menos 5.000 eventos; genera JSON pertinente, guarda JSON Lines y presenta conteos y manejo básico de errores. Los códigos se ejecutan siguiendo las instrucciones. | Completa el envío, recepción y archivo con el volumen requerido, con omisiones menores en registros, errores o instrucciones. | Implementa un flujo parcial, no acredita el volumen o necesita ajustes no documentados para guardar los mensajes. | No demuestra un flujo funcional de publicación, recepción y almacenamiento. |
| **Carga y preparación con PySpark** | Carga el archivo, revisa esquema y tipos, convierte la fecha, trata justificadamente la anomalía y muestra conteos antes y después. | Completa la carga y preparación principal, con una omisión menor de verificación o explicación. | Carga los datos, pero omite conversiones o aplica un tratamiento sin justificación ni comparación suficiente. | No carga el archivo con PySpark o no demuestra preparación de los datos. |
| **Análisis e interpretación** | Realiza con PySpark los tres análisis, presenta tres gráficos claros, responde las preguntas y reconoce las limitaciones de los datos sintéticos. | Responde las tres preguntas y presenta los gráficos, con interpretación limitada en alguno de los resultados. | Omite una pregunta o gráfico, o presenta resultados con interpretación superficial y uso limitado de PySpark. | No presenta un análisis verificable ni respuestas sustentadas en los datos. |
| **Presentación, demo y entregables** | Presentación sintética y completa; todos participan; demuestra el flujo y entrega archivos organizados con instrucciones suficientes. | Comunica y demuestra las etapas principales, con dificultades menores de organización o documentación. | Omite etapas relevantes de la demo, presenta participación desigual o entrega instrucciones incompletas. | No permite verificar el funcionamiento ni entrega los archivos esenciales para revisar el trabajo. |
| **Comprensión individual** | Explica autónomamente el flujo, su código y los resultados; justifica las decisiones y responde correctamente una verificación pertinente. | Explica las etapas principales con imprecisiones menores y apoyo ocasional. | Conoce principalmente su tarea y necesita apoyo reiterado para explicar otras etapas o resultados. | No logra explicar los componentes ni interpretar los resultados del proyecto. |