from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    to_timestamp,
    window,
    avg
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

# Crear la sesión de Spark
spark = (
    SparkSession.builder
    .appName("Tarea3_Spark_Streaming_Kafka")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Estructura de los mensajes JSON recibidos desde Kafka
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Leer el tema de Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "192.168.80.21:9092")
    .option("subscribe", "sensor_data")
    .option("startingOffsets", "latest")
    .load()
)

# Convertir el contenido binario a texto y analizar el JSON
datos = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), schema).alias("dato"))
    .select("dato.*")
    .withColumn("event_time", to_timestamp(col("timestamp")))
    .filter(
        col("sensor_id").isNotNull()
        & col("temperature").isNotNull()
        & col("humidity").isNotNull()
        & col("event_time").isNotNull()
    )
)

# Calcular promedios por sensor en ventanas de un minuto
promedios = (
    datos
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("sensor_id")
    )
    .agg(
        avg("temperature").alias("promedio_temperatura"),
        avg("humidity").alias("promedio_humedad")
    )
)

# Mostrar los resultados en la consola
consulta = (
    promedios.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .trigger(processingTime="10 seconds")
    .start()
)

consulta.awaitTermination()
