# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Tipificación y controles Silver
# MAGIC Lee las tablas Bronze, construye DataFrames tipificados y solo publica `aerolinea.silver` después de superar los controles bloqueantes.
# MAGIC
# MAGIC Las celdas SQL de la guía (vistas temporales `*_tipadas`, `control_*` y `CREATE OR REPLACE TABLE`) están **traducidas a PySpark**: cada vista temporal es aquí un DataFrame y cada `CREATE OR REPLACE TABLE ... AS SELECT` es un `write.mode("overwrite").saveAsTable`.

# COMMAND ----------

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def try_cast(columna, tipo):
    """TRY_CAST de SQL: devuelve NULL cuando el texto no puede convertirse (también con ANSI activo)."""
    return F.expr(f"try_cast({columna} AS {tipo})")


def llave(columna):
    """Quita espacios y pasa a mayúsculas: UPPER(TRIM(col))."""
    return F.upper(F.trim(F.col(columna)))


def vacio(columna):
    """col IS NULL OR col = ''"""
    return F.col(columna).isNull() | (F.col(columna) == "")


def informe(nombre_columna, filas):
    """Une en un DataFrame (conjunto/regla, errores) los conteos de cada regla, como los UNION ALL de la guía."""
    return reduce(DataFrame.unionByName, [
        df.agg(F.count("*").alias("errores")).select(F.lit(nombre).alias(nombre_columna), "errores")
        for nombre, df in filas
    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 · Tipificación de aeronaves
# MAGIC `distinct()` conserva una copia de filas iguales; los conflictos por llave se revisan en 2.6.

# COMMAND ----------

aeronaves_tipadas = (
    spark.table("aerolinea.bronze.aeronaves")
    .select(
        llave("aeronave_id").alias("aeronave_id"),
        F.trim("modelo").alias("modelo"),
        F.trim("fabricante").alias("fabricante"),
        try_cast("anio_fabricacion", "INT").alias("anio_fabricacion"),
        "archivo_origen",
    )
    .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 · Tipificación de aeropuertos

# COMMAND ----------

aeropuertos_tipados = (
    spark.table("aerolinea.bronze.aeropuertos")
    .select(
        llave("aeropuerto_id").alias("aeropuerto_id"),
        F.trim("nombre").alias("nombre"),
        F.trim("ciudad").alias("ciudad"),
        F.trim("pais").alias("pais"),
        try_cast("lat", "DOUBLE").alias("lat"),
        try_cast("lon", "DOUBLE").alias("lon"),
        "archivo_origen",
    )
    .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 · Tipificación de vuelos
# MAGIC `ruta_por_revisar` señala las filas con origen y destino iguales. La marca permanece visible y no se usa para eliminar filas.

# COMMAND ----------

vuelos_tipados = (
    spark.table("aerolinea.bronze.vuelos")
    .select(
        llave("vuelo_id").alias("vuelo_id"),
        try_cast("fecha", "DATE").alias("fecha"),
        llave("origen_id").alias("origen_id"),
        llave("destino_id").alias("destino_id"),
        llave("aeronave_id").alias("aeronave_id"),
        F.lower(F.trim("estado")).alias("estado"),
        try_cast("duracion_min", "INT").alias("duracion_min"),
        (llave("origen_id") == llave("destino_id")).alias("ruta_por_revisar"),
        "archivo_origen",
    )
    .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 · Tipificación de mantenimientos
# MAGIC El costo queda en `DECIMAL(14,2)` y la duración en horas `DECIMAL(10,2)`.

# COMMAND ----------

mantenimientos_tipados = (
    spark.table("aerolinea.bronze.mantenimientos")
    .select(
        llave("mantenimiento_id").alias("mantenimiento_id"),
        llave("aeronave_id").alias("aeronave_id"),
        try_cast("fecha", "DATE").alias("fecha"),
        F.lower(F.trim("tipo")).alias("tipo"),
        try_cast("costo_usd", "DECIMAL(14,2)").alias("costo_usd"),
        try_cast("duracion_hr", "DECIMAL(10,2)").alias("duracion_hr"),
        "archivo_origen",
    )
    .distinct()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 · Reglas de contenido
# MAGIC Cada fila cuenta los registros que incumplen una regla; esperamos cero. Una duración ausente o cero solo se permite en un vuelo cancelado; una duración negativa siempre es error.

# COMMAND ----------

anio_actual = F.year(F.current_date())

errores_aeronaves = aeronaves_tipadas.filter(
    vacio("aeronave_id") | vacio("modelo") | vacio("fabricante")
    | F.col("anio_fabricacion").isNull()
    | (F.col("anio_fabricacion") < 1900) | (F.col("anio_fabricacion") > anio_actual)
)

errores_aeropuertos = aeropuertos_tipados.filter(
    vacio("aeropuerto_id") | vacio("nombre") | vacio("ciudad") | vacio("pais")
    | F.col("lat").isNull() | F.col("lon").isNull()
    | F.isnan("lat") | F.isnan("lon")
    | ~F.col("lat").between(-90, 90) | ~F.col("lon").between(-180, 180)
)

errores_vuelos = vuelos_tipados.filter(
    vacio("vuelo_id") | F.col("fecha").isNull()
    | vacio("aeronave_id") | vacio("origen_id") | vacio("destino_id")
    | F.col("estado").isNull() | ~F.col("estado").isin("a_tiempo", "retrasado", "cancelado")
    | (F.col("duracion_min") < 0)
    | ((F.col("estado") != "cancelado")
       & (F.col("duracion_min").isNull() | (F.col("duracion_min") == 0)))
)

errores_mantenimientos = mantenimientos_tipados.filter(
    vacio("mantenimiento_id") | F.col("fecha").isNull()
    | vacio("aeronave_id") | F.col("tipo").isNull()
    | ~F.col("tipo").isin("preventivo", "correctivo", "inspección")
    | F.col("costo_usd").isNull() | (F.col("costo_usd") < 0)
    | F.col("duracion_hr").isNull() | (F.col("duracion_hr") <= 0)
)

control_contenido = informe("conjunto", [
    ("aeronaves", errores_aeronaves),
    ("aeropuertos", errores_aeropuertos),
    ("vuelos", errores_vuelos),
    ("mantenimientos", errores_mantenimientos),
])

# COMMAND ----------

display(control_contenido)
assert control_contenido.filter("errores > 0").count() == 0, "Revisar el contenido antes de publicar Silver."

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 · Conflictos por llave
# MAGIC Después de quitar filas iguales, una llave repetida significa valores distintos para el mismo registro. No elegimos una fila arbitrariamente.

# COMMAND ----------

def llaves_repetidas(df, columna):
    return df.groupBy(columna).count().filter(F.col("count") > 1)

control_llaves = informe("conjunto", [
    ("aeronaves", llaves_repetidas(aeronaves_tipadas, "aeronave_id")),
    ("aeropuertos", llaves_repetidas(aeropuertos_tipados, "aeropuerto_id")),
    ("vuelos", llaves_repetidas(vuelos_tipados, "vuelo_id")),
    ("mantenimientos", llaves_repetidas(mantenimientos_tipados, "mantenimiento_id")),
])

# COMMAND ----------

display(control_llaves)
assert control_llaves.filter("errores > 0").count() == 0, "Hay llaves con registros diferentes."

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.7 · Referencias y coherencia temporal
# MAGIC `left_anti` devuelve las filas sin correspondencia. También comprobamos que la aeronave no se haya fabricado después del evento.

# COMMAND ----------

v = vuelos_tipados.alias("v")
m = mantenimientos_tipados.alias("m")
a = aeronaves_tipadas.alias("a")
ap = aeropuertos_tipados.alias("ap")

control_referencias = informe("regla", [
    ("vuelo_aeronave",
     v.join(a, F.col("v.aeronave_id") == F.col("a.aeronave_id"), "left_anti")),
    ("vuelo_origen",
     v.join(ap, F.col("v.origen_id") == F.col("ap.aeropuerto_id"), "left_anti")),
    ("vuelo_destino",
     v.join(ap, F.col("v.destino_id") == F.col("ap.aeropuerto_id"), "left_anti")),
    ("mantenimiento_aeronave",
     m.join(a, F.col("m.aeronave_id") == F.col("a.aeronave_id"), "left_anti")),
    ("fabricacion_posterior_vuelo",
     v.join(a, F.col("v.aeronave_id") == F.col("a.aeronave_id"))
      .filter(F.col("a.anio_fabricacion") > F.year("v.fecha"))),
    ("fabricacion_posterior_mantenimiento",
     m.join(a, F.col("m.aeronave_id") == F.col("a.aeronave_id"))
      .filter(F.col("a.anio_fabricacion") > F.year("m.fecha"))),
])

# COMMAND ----------

display(control_referencias)
assert control_referencias.filter("errores > 0").count() == 0, "Revisar las referencias o los años de fabricación."

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.8 · Observaciones visibles
# MAGIC Con estos archivos aparecen **25 rutas por revisar**. La marca se mantiene en Silver y Gold.

# COMMAND ----------

rutas_revision = vuelos_tipados.filter("ruta_por_revisar")
print("rutas_por_revisar:", rutas_revision.count())
display(
    rutas_revision.select("vuelo_id", "fecha", "origen_id", "destino_id", "estado")
    .orderBy("vuelo_id").limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.9 · Publicación Silver
# MAGIC Solo llegamos aquí si pasaron los tres controles. Cada tabla es una escritura independiente; ante un fallo, corregir y repetir la tarea completa antes de seguir a Gold.

# COMMAND ----------

def publicar(df, tabla):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(tabla)

publicar(aeronaves_tipadas, "aerolinea.silver.aeronaves")

# COMMAND ----------

publicar(aeropuertos_tipados, "aerolinea.silver.aeropuertos")

# COMMAND ----------

publicar(vuelos_tipados, "aerolinea.silver.vuelos")

# COMMAND ----------

publicar(mantenimientos_tipados, "aerolinea.silver.mantenimientos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comprobación de Silver
# MAGIC Los conteos siguen siendo 100, 10, 5 y 100, y `vuelos` conserva `ruta_por_revisar`.

# COMMAND ----------

conteos_silver = reduce(DataFrame.unionByName, [
    spark.table(f"aerolinea.silver.{t}").agg(F.count("*").alias("registros"))
    .select(F.lit(t).alias("conjunto"), "registros")
    for t in ["vuelos", "aeronaves", "aeropuertos", "mantenimientos"]
])
display(conteos_silver)

obtenidos = {r["conjunto"]: r["registros"] for r in conteos_silver.collect()}
assert obtenidos == {"vuelos": 100, "aeronaves": 10, "aeropuertos": 5, "mantenimientos": 100}, obtenidos
assert "ruta_por_revisar" in spark.table("aerolinea.silver.vuelos").columns
