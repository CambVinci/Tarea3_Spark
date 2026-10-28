# Tarea 3: Procesamiento de Datos con Apache Spark

Proyecto académico de Big Data de la UNAD (curso 202016911).

## Objetivo
Implementar procesamiento por lotes con PySpark sobre datos históricos de la TRM almacenados en HDFS y procesamiento en tiempo real con Kafka y Spark Structured Streaming.

## Fuente de datos históricos
https://www.datos.gov.co/Econom-a-y-Finanzas/Tasa-de-Cambio-Representativa-del-Mercado-Historic/mcec-87by

## Tecnologías
- Ubuntu Server 22.04.5 LTS
- Hadoop 3.3.6
- Spark 3.5.8
- Kafka 3.9.2
- Python y PySpark

## Estructura
- batch/tarea3.py: análisis histórico por lotes.
- streaming/productor_sensores.py: generación de datos simulados.
- streaming/streaming_tarea3.py: análisis de flujo con Spark.
- resultados/resultado_tarea3.txt: salida del procesamiento por lotes.

## Procesamiento por lotes
El script inspecciona el esquema y los valores nulos, filtra registros incompletos, conserva valores positivos en COP, transforma fechas y calcula estadísticas descriptivas.

Para ejecutarlo, HDFS debe estar activo, el archivo debe existir en /Tarea3/rows.csv y Spark Master debe estar disponible en spark://192.168.80.21:7077.

```bash
/opt/spark/bin/spark-submit /home/hadoop/Tarea3_Spark/batch/tarea3.py
```

## Procesamiento en tiempo real
El productor genera registros JSON simulados de sensores. Kafka los publica en el tópico sensor_data y Spark Structured Streaming calcula promedios por sensor y ventana temporal.

Iniciar el productor en una terminal:
```bash
python3 /home/hadoop/Tarea3_Spark/streaming/productor_sensores.py
```

Iniciar Spark en otra terminal:
```bash
/opt/spark/bin/spark-submit --conf spark.jars.ivySettings=/home/hadoop/spark-ivy-settings.xml --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 /home/hadoop/Tarea3_Spark/streaming/streaming_tarea3.py
```

## Resultados
El procesamiento por lotes se ejecutó sobre 8.354 registros y conservó la cantidad de registros tras la limpieza. El procesamiento en tiempo real mostró promedios por sensor y ventana. Los datos de sensores son simulados.

## Autoría
Completar con los nombres de los integrantes que contribuyeron al proyecto.
