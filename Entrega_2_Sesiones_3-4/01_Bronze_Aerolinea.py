# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Ingesta Bronze
# MAGIC Lee los cuatro CSV de `/Volumes/aerolinea/fuentes/landing` y los publica como tablas Delta en `aerolinea.bronze`.
# MAGIC
# MAGIC Todos los campos de negocio se conservan como **texto**. `FAILFAST` detiene la carga si un registro no tiene la estructura CSV esperada. Cada tabla añade `archivo_origen` y `fecha_ingesta`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 · Ruta y funciones
# MAGIC Esta tarea define sus propias variables; no usa la memoria del notebook anterior.

# COMMAND ----------

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ruta = "/Volumes/aerolinea/fuentes/landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 · Lectura de vuelos

# COMMAND ----------

vuelos_bronze = (
    spark.read.csv(
        ruta + "/vuelos_diarios.csv", header=True,
        schema="""vuelo_id STRING, fecha STRING, origen_id STRING, destino_id STRING,
                  aeronave_id STRING, estado STRING, duracion_min STRING""",
        mode="FAILFAST", enforceSchema=False
    )
    .withColumn("archivo_origen", F.lit("vuelos_diarios.csv"))
    .withColumn("fecha_ingesta", F.current_timestamp())
)
display(vuelos_bronze.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.7 · Lectura de aeronaves

# COMMAND ----------

aeronaves_bronze = (
    spark.read.csv(
        ruta + "/aeronaves.csv", header=True,
        schema="aeronave_id STRING, modelo STRING, fabricante STRING, anio_fabricacion STRING",
        mode="FAILFAST", enforceSchema=False
    )
    .withColumn("archivo_origen", F.lit("aeronaves.csv"))
    .withColumn("fecha_ingesta", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.8 · Lectura de aeropuertos

# COMMAND ----------

aeropuertos_bronze = (
    spark.read.csv(
        ruta + "/aeropuertos.csv", header=True,
        schema="aeropuerto_id STRING, nombre STRING, ciudad STRING, pais STRING, lat STRING, lon STRING",
        mode="FAILFAST", enforceSchema=False
    )
    .withColumn("archivo_origen", F.lit("aeropuertos.csv"))
    .withColumn("fecha_ingesta", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.9 · Lectura de mantenimientos

# COMMAND ----------

mantenimientos_bronze = (
    spark.read.csv(
        ruta + "/mantenimientos_rds.csv", header=True,
        schema="""mantenimiento_id STRING, aeronave_id STRING, fecha STRING,
                  tipo STRING, costo_usd STRING, duracion_hr STRING""",
        mode="FAILFAST", enforceSchema=False
    )
    .withColumn("archivo_origen", F.lit("mantenimientos_rds.csv"))
    .withColumn("fecha_ingesta", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.10 · Revisión antes de escribir
# MAGIC Conteos de referencia de esta instantánea: 100, 10, 5 y 100. `printSchema` muestra los campos del CSV como `string` y `fecha_ingesta` como `timestamp`.

# COMMAND ----------

print("Vuelos:", vuelos_bronze.count())
print("Aeronaves:", aeronaves_bronze.count())
print("Aeropuertos:", aeropuertos_bronze.count())
print("Mantenimientos:", mantenimientos_bronze.count())
vuelos_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.11 · Escritura Bronze
# MAGIC Cada escritura reemplaza la tabla completa (`overwrite`): repetir la tarea reconstruye la instantánea en lugar de añadir copias. No se modifica el archivo del volumen.

# COMMAND ----------

vuelos_bronze.write.format("delta").mode("overwrite").saveAsTable("aerolinea.bronze.vuelos")

# COMMAND ----------

aeronaves_bronze.write.format("delta").mode("overwrite").saveAsTable("aerolinea.bronze.aeronaves")

# COMMAND ----------

aeropuertos_bronze.write.format("delta").mode("overwrite").saveAsTable("aerolinea.bronze.aeropuertos")

# COMMAND ----------

mantenimientos_bronze.write.format("delta").mode("overwrite").saveAsTable("aerolinea.bronze.mantenimientos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comprobación de Bronze
# MAGIC Equivale a la consulta SQL `SELECT 'vuelos' AS fuente, COUNT(*) ... UNION ALL ...` de la guía.

# COMMAND ----------

def conteo(fuente, tabla):
    return spark.table(tabla).agg(F.count("*").alias("registros")).select(
        F.lit(fuente).alias("fuente"), "registros"
    )

conteos_bronze = reduce(DataFrame.unionByName, [
    conteo("vuelos", "aerolinea.bronze.vuelos"),
    conteo("aeronaves", "aerolinea.bronze.aeronaves"),
    conteo("aeropuertos", "aerolinea.bronze.aeropuertos"),
    conteo("mantenimientos", "aerolinea.bronze.mantenimientos"),
])
display(conteos_bronze)

esperados = {"vuelos": 100, "aeronaves": 10, "aeropuertos": 5, "mantenimientos": 100}
obtenidos = {r["fuente"]: r["registros"] for r in conteos_bronze.collect()}
assert obtenidos == esperados, f"Conteos Bronze inesperados: {obtenidos}"
