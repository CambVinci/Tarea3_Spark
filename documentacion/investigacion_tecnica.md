# Tarea 3. Procesamiento de datos con Apache Spark

## 1. Introducción

El procesamiento de grandes volúmenes de datos requiere herramientas que permitan almacenar, transformar y analizar información de manera eficiente. Apache Hadoop proporciona componentes para el almacenamiento distribuido y la gestión de recursos, mientras que Apache Spark permite ejecutar procesos de análisis de datos mediante diferentes modelos de procesamiento.

En esta actividad se implementa una solución que integra Apache Spark, Hadoop y Apache Kafka. Se utilizan datos históricos de la Tasa Representativa del Mercado (TRM) para realizar procesamiento por lotes y se desarrolla un flujo de datos en tiempo real con información simulada de sensores.

El proyecto permite aplicar conceptos de procesamiento distribuido, transformación de datos, análisis exploratorio, visualización y procesamiento de flujos.

## 2. Objetivos

### 2.1. Objetivo general

Implementar una solución de procesamiento de datos mediante Apache Spark que permita analizar información histórica y procesar datos en tiempo real, utilizando herramientas del ecosistema de datos distribuidos.

### 2.2. Objetivos específicos

- Identificar las características y diferencias entre Hadoop y Apache Spark.
- Comprender los conceptos fundamentales de RDD, transformaciones, acciones y DataFrames.
- Reconocer la función de Apache Kafka en la transmisión de datos.
- Aplicar procesamiento por lotes a un conjunto de datos histórico de la TRM.
- Implementar un flujo de procesamiento en tiempo real con datos simulados de sensores.
- Generar resultados y visualizaciones que faciliten la interpretación de los datos.

## 3. Comparación entre Hadoop y Apache Spark

| Característica | Apache Hadoop | Apache Spark |
|---|---|---|
| Propósito principal | Almacenamiento distribuido y procesamiento de datos. | Procesamiento y análisis de datos distribuidos. |
| Almacenamiento | Hadoop Distributed File System (HDFS). | Puede utilizar HDFS y otras fuentes de datos. |
| Procesamiento | MapReduce procesa tareas mediante etapas que suelen utilizar disco. | Utiliza un motor de ejecución que puede mantener datos en memoria. |
| Rendimiento | Adecuado para procesamiento distribuido por lotes. | Puede ofrecer mayor velocidad en cargas iterativas y análisis interactivos. |
| Procesamiento en tiempo real | Requiere herramientas adicionales para flujos de datos. | Spark Structured Streaming permite procesar flujos de datos. |
| Lenguajes | Principalmente Java, además de otras interfaces. | Scala, Python, Java y R. |
| Uso conjunto | Proporciona almacenamiento y administración de recursos. | Puede procesar datos almacenados en HDFS. |

Hadoop y Spark no son necesariamente tecnologías competidoras. Pueden complementarse: Hadoop permite almacenar datos distribuidos mediante HDFS, mientras Spark realiza operaciones de procesamiento y análisis sobre esos datos.

## 4. Conceptos fundamentales de Apache Spark

### 4.1. Resilient Distributed Dataset (RDD)

Un RDD es una colección distribuida e inmutable de elementos que Spark puede procesar en paralelo. Sus particiones pueden distribuirse entre diferentes nodos de un clúster.

Los RDD permiten aplicar operaciones sobre colecciones de datos y ofrecen mecanismos de recuperación mediante el registro de las operaciones necesarias para reconstruir particiones perdidas.

### 4.2. Transformaciones y acciones

Las transformaciones definen operaciones que generan un nuevo conjunto de datos a partir de otro. Entre ellas se encuentran:

- \`map()\`: aplica una función a cada elemento.
- \`filter()\`: conserva los elementos que cumplen una condición.
- \`flatMap()\`: permite producir cero o varios elementos por cada elemento de entrada.

Las acciones ejecutan el trabajo y permiten obtener resultados. Algunos ejemplos son:

- \`count()\`: cuenta los elementos.
- \`collect()\`: devuelve los elementos al programa controlador.
- \`reduce()\`: combina los elementos mediante una función.

Spark utiliza evaluación perezosa para muchas transformaciones: el trabajo se planifica y ejecuta cuando una acción requiere los resultados.

## 5. DataFrames en Apache Spark

Un DataFrame es una estructura de datos distribuida organizada en columnas, similar a una tabla de una base de datos relacional. Cada columna tiene un nombre y un tipo de dato.

Los DataFrames facilitan la consulta, transformación y agregación de información mediante operaciones de alto nivel. Spark puede optimizar estas operaciones mediante su motor de ejecución.

Entre las operaciones más utilizadas se encuentran:

- `select()`: selecciona las columnas necesarias.
- `filter()`: filtra registros según una condición.
- `groupBy()`: agrupa los registros por una o varias columnas.
- `agg()`: calcula agregaciones como promedios, sumas y conteos.
- `orderBy()`: ordena los resultados.

### 5.1. Diferencias entre RDD y DataFrame

Los RDD proporcionan un nivel de control más bajo sobre los elementos distribuidos. Los DataFrames organizan la información en columnas y permiten que Spark optimice las operaciones.

Para tareas de análisis estructurado, los DataFrames suelen ser más prácticos porque facilitan la lectura del código y permiten utilizar optimizaciones automáticas.

## 6. Apache Kafka y el procesamiento de datos en tiempo real

Apache Kafka es una plataforma distribuida para publicar, transmitir y consumir flujos de eventos. Permite que los productores envíen datos y que los consumidores los procesen de forma desacoplada.

Sus conceptos principales son:

- **Productor:** aplicación que publica eventos en Kafka.
- **Consumidor:** aplicación que lee los eventos publicados.
- **Tópico:** categoría donde se organizan los eventos.
- **Broker:** servidor Kafka que recibe, almacena y distribuye eventos.
- **Partición:** división de un tópico que permite distribuir los datos.
- **Offset:** posición de un evento dentro de una partición.

Kafka permite separar la generación de datos de su procesamiento. Por ejemplo, una aplicación puede enviar mediciones de sensores a un tópico y otra aplicación puede consumirlas para calcular estadísticas.

### 6.1. Integración de Kafka con Spark Structured Streaming

Spark Structured Streaming permite procesar flujos de datos utilizando una API basada en DataFrames. Al conectarse con Kafka, Spark puede leer eventos continuamente, interpretar sus contenidos y aplicar transformaciones y agregaciones.

En este proyecto, el productor genera registros de sensores en formato JSON con identificador, temperatura, humedad y marca de tiempo. Los eventos se publican en el tópico `sensor_data`, desde el cual Spark los consume y calcula promedios por sensor y ventana temporal.

Este flujo demuestra cómo integrar la generación, transmisión y procesamiento de datos para obtener resultados periódicos a partir de información que llega continuamente.

## 7. Descripción del conjunto de datos

### 7.1. Datos históricos de la TRM

Para el procesamiento por lotes se utiliza un conjunto de datos histórico de la Tasa Representativa del Mercado (TRM) de Colombia, publicado en el portal de datos abiertos del Gobierno colombiano.

Fuente: https://www.datos.gov.co/Econom-a-y-Finanzas/Tasa-de-Cambio-Representativa-del-Mercado-Historic/mcec-87by

El conjunto contiene registros históricos con información sobre el valor de la tasa de cambio, la unidad y las fechas de vigencia. Para el análisis se utilizan los siguientes campos:

- `VALOR`: valor de la tasa de cambio.
- `UNIDAD`: unidad asociada al dato.
- `VIGENCIADESDE`: fecha inicial de vigencia.
- `VIGENCIAHASTA`: fecha final de vigencia.

### 7.2. Procesamiento por lotes

El procesamiento por lotes permite analizar un conjunto de datos almacenado y obtener resultados a partir de la información disponible.

En este proyecto, los registros históricos se procesan con Apache Spark. El flujo contempla la lectura de los datos, la revisión de su estructura, las transformaciones necesarias y el cálculo de estadísticas descriptivas.

Entre los resultados obtenidos se encuentran el número de registros analizados, el periodo cubierto por los datos y los valores mínimo, máximo y promedio de la TRM.

El análisis también se complementa con visualizaciones para facilitar la interpretación de la evolución temporal y la distribución de los valores.

### 7.3. Limpieza y análisis exploratorio

Antes de interpretar los resultados, es importante verificar que los datos tengan la estructura esperada y que los valores utilizados sean válidos. Las comprobaciones pueden incluir la revisión de valores nulos, la conversión de tipos de datos y la validación de las fechas y los valores numéricos.

El análisis exploratorio permite describir el comportamiento general de los datos e identificar variaciones, valores extremos y patrones temporales. Los resultados deben interpretarse teniendo en cuenta el periodo estudiado y las características de la fuente.

## 8. Procesamiento de datos de sensores

Para el procesamiento en tiempo real se utiliza un productor que simula mediciones de varios sensores. Cada evento contiene un identificador de sensor, temperatura, humedad y marca de tiempo, y se transmite en formato JSON a Kafka.

Spark Structured Streaming consume los eventos del tópico `sensor_data` y calcula promedios agrupados por sensor y ventana temporal. Las ventanas permiten resumir las mediciones recibidas durante intervalos definidos.

Este ejercicio ilustra la diferencia entre el análisis de un conjunto histórico almacenado y el procesamiento continuo de eventos. En el primer caso se estudia información disponible; en el segundo, se procesan mediciones a medida que llegan al sistema.

## 9. Referencias bibliográficas

Apache Software Foundation. (s. f.). *Apache Hadoop*. https://hadoop.apache.org/

Apache Software Foundation. (s. f.). *Apache Spark Documentation*. https://spark.apache.org/docs/latest/

Apache Software Foundation. (s. f.). *Structured Streaming Programming Guide*. https://spark.apache.org/docs/latest/streaming/index.html

Apache Software Foundation. (s. f.). *Apache Kafka Documentation*. https://kafka.apache.org/documentation/

Ministerio de Tecnologías de la Información y las Comunicaciones. (s. f.). *Datos Abiertos Colombia*. https://www.datos.gov.co/
