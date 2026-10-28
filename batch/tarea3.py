
from pyspark.sql import SparkSession, functions as F

# 1. Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("Tarea3_Analisis_Historico") \
    .master("spark://192.168.80.21:7077") \
    .getOrCreate()

# 2. Leer el conjunto de datos desde HDFS
file_path = "hdfs://localhost:9000/Tarea3/rows.csv"

df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(file_path)

print("\n===== 1. ESQUEMA ORIGINAL =====")
df.printSchema()

print("\n===== 2. MUESTRA DE DATOS ORIGINALES =====")
df.show(5, truncate=False)

print("\n===== 3. CALIDAD DE LOS DATOS =====")
print("Registros originales:", df.count())

df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in df.columns
]).show()

# 3. Limpieza: eliminar registros con campos esenciales nulos
df_limpio = df.dropna(
    subset=["VALOR", "UNIDAD", "VIGENCIADESDE", "VIGENCIAHASTA"]
)

# Eliminar valores no numéricos, nulos o menores/iguales a cero
df_limpio = df_limpio.filter(
    (F.col("VALOR") > 0) &
    (F.col("UNIDAD") == "COP")
)

# 4. Transformación: convertir las fechas de texto a tipo fecha
df_limpio = df_limpio \
    .withColumn(
        "FECHA_DESDE",
        F.to_date(F.col("VIGENCIADESDE"), "dd/MM/yyyy")
    ) \
    .withColumn(
        "FECHA_HASTA",
        F.to_date(F.col("VIGENCIAHASTA"), "dd/MM/yyyy")
    )

# Excluir registros con fechas inválidas
df_limpio = df_limpio.filter(
    F.col("FECHA_DESDE").isNotNull() &
    F.col("FECHA_HASTA").isNotNull()
)

print("\n===== 5. RESULTADO DE LA LIMPIEZA =====")
print("Registros después de la limpieza:", df_limpio.count())

print("\n===== 6. ESQUEMA TRANSFORMADO =====")
df_limpio.printSchema()

print("\n===== 7. ESTADÍSTICAS DESCRIPTIVAS =====")
df_limpio.select("VALOR").describe().show()

print("\n===== 8. VALOR MÍNIMO Y MÁXIMO =====")
df_limpio.agg(
    F.min("VALOR").alias("Valor_minimo"),
    F.max("VALOR").alias("Valor_maximo"),
    F.avg("VALOR").alias("Promedio"),
    F.stddev("VALOR").alias("Desviacion_estandar")
).show()

print("\n===== 9. CINCO VALORES MÁS ALTOS =====")
df_limpio.orderBy(
    F.col("VALOR").desc()
).select(
    "VALOR", "FECHA_DESDE", "FECHA_HASTA"
).show(5, truncate=False)

print("\n===== 10. REGISTROS CON VALOR SUPERIOR A 5000 =====")
df_limpio.filter(
    F.col("VALOR") > 5000
).select(
    "VALOR", "FECHA_DESDE", "FECHA_HASTA"
).orderBy(
    F.col("VALOR").desc()
).show(truncate=False)

print("\n===== 11. EVOLUCIÓN CRONOLÓGICA: PRIMEROS REGISTROS =====")
df_limpio.orderBy(
    "FECHA_DESDE"
).select(
    "FECHA_DESDE", "VALOR"
).show(10, truncate=False)

# 5. Cerrar la sesión
spark.stop()
