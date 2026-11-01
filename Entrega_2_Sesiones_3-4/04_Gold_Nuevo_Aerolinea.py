# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold nuevo: indicadores por aeronave, ruta y fabricante + control final
# MAGIC Construye cuatro tablas Gold nuevas **a partir de las tablas Gold y Silver ya publicadas** por `03_Gold_Aerolinea`, usando PySpark y funciones de ventana (`Window`):
# MAGIC
# MAGIC | Tabla | Grano | Para qué sirve |
# MAGIC |---|---|---|
# MAGIC | `aerolinea.gold.kpi_aeronave` | 1 fila por aeronave | Actividad, puntualidad y costo de cada avión; costo por vuelo operado y por hora volada; ranking y participación en el costo total |
# MAGIC | `aerolinea.gold.kpi_rutas` | 1 fila por origen–destino | Volumen y puntualidad de cada ruta y su posición dentro del aeropuerto de origen |
# MAGIC | `aerolinea.gold.kpi_mantenimiento_fabricante` | 1 fila por fabricante y tipo | Peso de cada tipo de intervención dentro del gasto de cada fabricante |
# MAGIC | `aerolinea.gold.costo_acumulado_aeronave` | 1 fila por intervención | Costo acumulado de cada aeronave en el tiempo y días desde la intervención anterior |
# MAGIC
# MAGIC Al final ejecuta los **controles del pipeline** (conteos, reconciliación de eventos y costos, coherencia de estados) y el diagnóstico (plan, `DESCRIBE DETAIL`, `DESCRIBE HISTORY`). Si un control falla, la tarea termina en *Failed*.
# MAGIC
# MAGIC Esta tarea solo lee tablas persistentes: no usa variables ni vistas temporales de otros notebooks.

# COMMAND ----------

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def publicar(df, tabla):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(tabla)


def contar_si(condicion):
    return F.sum(F.when(condicion, 1).otherwise(0))


def dividir(numerador, denominador):
    """numerador / NULLIF(denominador, 0): NULL cuando el denominador es 0 (sin error con ANSI)."""
    return F.when(denominador != 0, numerador / denominador)


def pct(numerador, denominador):
    return F.round(dividir(100.0 * numerador, denominador), 2)


ve = spark.table("aerolinea.gold.vuelos_enriquecidos")
me = spark.table("aerolinea.gold.mantenimientos_enriquecidos")
aeronaves = spark.table("aerolinea.silver.aeronaves")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 · KPI por aeronave
# MAGIC Igual que en 3.8, primero resumimos vuelos y mantenimientos **por separado** al nivel de aeronave y después los unimos, para no multiplicar filas. Partimos de la dimensión `aeronaves` para que aparezca también un avión sin eventos.
# MAGIC
# MAGIC - `costo_por_vuelo_operado` y `costo_por_hora_volada` usan `dividir()` (equivale a `/ NULLIF(x, 0)`) para no dividir entre cero.
# MAGIC - `ranking_costo` (`dense_rank`) y `participacion_costo_pct` (`sum` sobre una ventana sin partición) se calculan con `Window`.

# COMMAND ----------

vuelos_aeronave = ve.groupBy("aeronave_id").agg(
    F.count("*").alias("total_vuelos"),
    contar_si(F.col("operado")).alias("vuelos_operados"),
    contar_si(F.col("estado") == "a_tiempo").alias("vuelos_a_tiempo"),
    contar_si(F.col("estado") == "cancelado").alias("vuelos_cancelados"),
    F.sum(F.when(F.col("operado"), F.col("duracion_min")).otherwise(0)).alias("minutos_volados"),
)

mantenimientos_aeronave = me.groupBy("aeronave_id").agg(
    F.count("*").alias("total_mantenimientos"),
    contar_si(F.col("tipo") == "correctivo").alias("mantenimientos_correctivos"),
    F.sum("costo_usd").alias("costo_total_usd"),
    F.sum("duracion_hr").alias("horas_en_mantenimiento"),
)

todo = Window.partitionBy()

kpi_aeronave = (
    aeronaves.select("aeronave_id", "modelo", "fabricante", "anio_fabricacion")
    .join(vuelos_aeronave, "aeronave_id", "left")
    .join(mantenimientos_aeronave, "aeronave_id", "left")
    .fillna(0, subset=[
        "total_vuelos", "vuelos_operados", "vuelos_a_tiempo", "vuelos_cancelados",
        "minutos_volados", "total_mantenimientos", "mantenimientos_correctivos",
    ])
    .withColumn("costo_total_usd", F.coalesce("costo_total_usd", F.lit(0).cast("decimal(24,2)")))
    .withColumn("edad_aeronave_2024", F.lit(2024) - F.col("anio_fabricacion"))
    .withColumn("puntualidad_operados_pct", pct(F.col("vuelos_a_tiempo"), F.col("vuelos_operados")))
    .withColumn("cancelacion_pct", pct(F.col("vuelos_cancelados"), F.col("total_vuelos")))
    .withColumn("horas_voladas", F.round(F.col("minutos_volados") / 60, 2))
    .withColumn("costo_por_vuelo_operado_usd",
                F.round(dividir(F.col("costo_total_usd"), F.col("vuelos_operados")), 2))
    .withColumn("costo_por_hora_volada_usd",
                F.round(dividir(F.col("costo_total_usd"), F.col("horas_voladas")), 2))
    .withColumn("ranking_costo", F.dense_rank().over(Window.orderBy(F.desc("costo_total_usd"))))
    .withColumn("participacion_costo_pct",
                F.round(100.0 * F.col("costo_total_usd") / F.sum("costo_total_usd").over(todo), 2))
)
publicar(kpi_aeronave, "aerolinea.gold.kpi_aeronave")

# COMMAND ----------

display(spark.table("aerolinea.gold.kpi_aeronave").orderBy("ranking_costo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 · KPI por ruta
# MAGIC `ranking_en_origen` ordena las rutas de cada aeropuerto de salida por número de vuelos (`row_number` sobre una ventana `partitionBy("origen_id")`). Las rutas con origen = destino conservan `ruta_por_revisar`.

# COMMAND ----------

por_origen = Window.partitionBy("origen_id").orderBy(F.desc("total_vuelos"), "destino_id")

kpi_rutas = (
    ve.groupBy("origen_id", "origen_ciudad", "destino_id", "destino_ciudad", "ruta_por_revisar")
    .agg(
        F.count("*").alias("total_vuelos"),
        contar_si(F.col("operado")).alias("vuelos_operados"),
        contar_si(F.col("estado") == "a_tiempo").alias("vuelos_a_tiempo"),
        contar_si(F.col("estado") == "cancelado").alias("vuelos_cancelados"),
        F.round(F.avg(F.when(F.col("operado"), F.col("duracion_min"))), 2).alias("duracion_promedio_operados_min"),
    )
    .withColumn("ruta", F.concat_ws(" → ", "origen_id", "destino_id"))
    .withColumn("puntualidad_operados_pct", pct(F.col("vuelos_a_tiempo"), F.col("vuelos_operados")))
    .withColumn("cancelacion_pct", pct(F.col("vuelos_cancelados"), F.col("total_vuelos")))
    .withColumn("ranking_en_origen", F.row_number().over(por_origen))
    .withColumn("participacion_origen_pct",
                pct(F.col("total_vuelos"), F.sum("total_vuelos").over(Window.partitionBy("origen_id"))))
)
publicar(kpi_rutas, "aerolinea.gold.kpi_rutas")

# COMMAND ----------

display(spark.table("aerolinea.gold.kpi_rutas").orderBy("origen_id", "ranking_en_origen"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 · Mantenimiento por fabricante y tipo
# MAGIC `participacion_en_fabricante_pct` divide el costo de cada tipo entre el costo total del fabricante (ventana `partitionBy("fabricante")`).

# COMMAND ----------

por_fabricante = Window.partitionBy("fabricante")

kpi_mantenimiento_fabricante = (
    me.groupBy("fabricante", "tipo")
    .agg(
        F.count("*").alias("total_mantenimientos"),
        F.countDistinct("aeronave_id").alias("aeronaves"),
        F.sum("costo_usd").alias("costo_total_usd"),
        F.round(F.avg("costo_usd"), 2).alias("costo_promedio_usd"),
        F.round(F.avg("duracion_hr"), 2).alias("duracion_promedio_hr"),
    )
    .withColumn("participacion_en_fabricante_pct",
                F.round(100.0 * F.col("costo_total_usd") / F.sum("costo_total_usd").over(por_fabricante), 2))
    .withColumn("ranking_en_fabricante",
                F.dense_rank().over(por_fabricante.orderBy(F.desc("costo_total_usd"))))
)
publicar(kpi_mantenimiento_fabricante, "aerolinea.gold.kpi_mantenimiento_fabricante")

# COMMAND ----------

display(spark.table("aerolinea.gold.kpi_mantenimiento_fabricante").orderBy("fabricante", "ranking_en_fabricante"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 · Costo acumulado por aeronave
# MAGIC Ventana ordenada por fecha dentro de cada aeronave: `sum(...).over(rowsBetween(unboundedPreceding, currentRow))` da el acumulado y `lag` la intervención anterior. La última fila de cada aeronave debe coincidir con su costo total en `kpi_aeronave`.

# COMMAND ----------

historial = Window.partitionBy("aeronave_id").orderBy("fecha", "mantenimiento_id")
acumulado = historial.rowsBetween(Window.unboundedPreceding, Window.currentRow)

costo_acumulado_aeronave = (
    me.select("aeronave_id", "modelo", "fabricante", "mantenimiento_id", "fecha", "tipo", "costo_usd")
    .withColumn("numero_intervencion", F.row_number().over(historial))
    .withColumn("costo_acumulado_usd", F.sum("costo_usd").over(acumulado))
    .withColumn("fecha_intervencion_anterior", F.lag("fecha").over(historial))
    .withColumn("dias_desde_anterior", F.datediff("fecha", "fecha_intervencion_anterior"))
)
publicar(costo_acumulado_aeronave, "aerolinea.gold.costo_acumulado_aeronave")

# COMMAND ----------

display(
    spark.table("aerolinea.gold.costo_acumulado_aeronave")
    .orderBy("aeronave_id", "numero_intervencion").limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control del pipeline
# MAGIC ### 4.5 · Conteos de referencia
# MAGIC Conteos fijos de la instantánea de clase. Para la prueba de **fallo controlado** del Job (guía 4.8), cambiar temporalmente `assert vuelos == 100` por `assert vuelos == 101`, ejecutar, y después restaurar y usar **Repair run**.

# COMMAND ----------

conteos = reduce(DataFrame.unionByName, [
    spark.table(t).agg(F.count("*").alias("registros")).select(F.lit(t.split(".", 1)[1]).alias("tabla"), "registros")
    for t in [
        "aerolinea.bronze.vuelos", "aerolinea.silver.vuelos",
        "aerolinea.gold.vuelos_enriquecidos", "aerolinea.gold.mantenimientos_enriquecidos",
        "aerolinea.gold.base_ml_mantenimiento", "aerolinea.gold.kpi_aeronave",
        "aerolinea.gold.kpi_rutas", "aerolinea.gold.kpi_mantenimiento_fabricante",
        "aerolinea.gold.costo_acumulado_aeronave",
    ]
])
display(conteos)

# COMMAND ----------

vuelos = spark.table("aerolinea.gold.vuelos_enriquecidos").count()
assert vuelos == 100, "El conteo no coincide con la instantánea de clase."
assert spark.table("aerolinea.gold.mantenimientos_enriquecidos").count() == 100
assert spark.table("aerolinea.gold.base_ml_mantenimiento").count() == 100
assert spark.table("aerolinea.gold.kpi_aeronave").count() == 10
assert spark.table("aerolinea.gold.kpi_rutas").count() == 25
assert spark.table("aerolinea.gold.kpi_mantenimiento_fabricante").count() == 9
assert spark.table("aerolinea.gold.costo_acumulado_aeronave").count() == 100

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 · Reconciliación de conteos y costos
# MAGIC El total de eventos y de costo debe coincidir entre el detalle Silver y cada resumen Gold (los de la guía y los nuevos).

# COMMAND ----------

def regla(nombre, esperado_df, esperado_col, obtenido_df, obtenido_col):
    esperado = esperado_df.agg(esperado_col.alias("v")).first()["v"]
    obtenido = obtenido_df.agg(obtenido_col.alias("v")).first()["v"]
    return (nombre, str(esperado), str(obtenido) if obtenido is not None else None)

silver_v = spark.table("aerolinea.silver.vuelos")
silver_m = spark.table("aerolinea.silver.mantenimientos")
gold = lambda t: spark.table(f"aerolinea.gold.{t}")

ultima_por_aeronave = (
    gold("costo_acumulado_aeronave")
    .withColumn("orden", F.row_number().over(
        Window.partitionBy("aeronave_id").orderBy(F.desc("numero_intervencion"))))
    .filter("orden = 1")
)

filas = [
    regla("vuelos_por_grupo", silver_v, F.count("*"), gold("kpi_puntualidad_vuelos"), F.sum("total_vuelos")),
    regla("mantenimientos_por_grupo", silver_m, F.count("*"), gold("kpi_costo_mantenimiento"), F.sum("total_mantenimientos")),
    regla("vuelos_por_aeronave_mes", silver_v, F.count("*"), gold("resumen_aeronave_mes"), F.sum("total_vuelos")),
    regla("mantenimientos_por_aeronave_mes", silver_m, F.count("*"), gold("resumen_aeronave_mes"), F.sum("total_mantenimientos")),
    regla("costo_por_grupo", silver_m, F.sum("costo_usd"), gold("kpi_costo_mantenimiento"), F.sum("costo_total_usd")),
    regla("costo_por_aeronave_mes", silver_m, F.sum("costo_usd"), gold("resumen_aeronave_mes"), F.sum("costo_total_usd")),
    regla("vuelos_por_aeronave", silver_v, F.count("*"), gold("kpi_aeronave"), F.sum("total_vuelos")),
    regla("costo_por_aeronave", silver_m, F.sum("costo_usd"), gold("kpi_aeronave"), F.sum("costo_total_usd")),
    regla("vuelos_por_ruta", silver_v, F.count("*"), gold("kpi_rutas"), F.sum("total_vuelos")),
    regla("costo_por_fabricante_tipo", silver_m, F.sum("costo_usd"), gold("kpi_mantenimiento_fabricante"), F.sum("costo_total_usd")),
    regla("costo_acumulado_final", silver_m, F.sum("costo_usd"), ultima_por_aeronave, F.sum("costo_acumulado_usd")),
]
reconciliacion = spark.createDataFrame(filas, "regla STRING, esperado STRING, obtenido STRING")
display(reconciliacion)
assert reconciliacion.filter("obtenido IS NULL OR esperado <> obtenido").count() == 0, "Un resumen no coincide con el detalle."

total_costo = silver_m.agg(F.sum("costo_usd")).first()[0]
print("Costo total de mantenimiento (USD):", total_costo)
assert str(total_costo) == "2589355.38", total_costo

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.7 · Revisión de las tasas
# MAGIC La suma de estados debe explicar los cien vuelos; las 25 rutas por revisar siguen visibles. Las participaciones de las ventanas deben sumar ~100 %.

# COMMAND ----------

estados = (
    gold("kpi_puntualidad_vuelos")
    .agg(
        F.sum("total_vuelos").alias("total"),
        F.sum("vuelos_operados").alias("operados"),
        F.sum("vuelos_a_tiempo").alias("a_tiempo"),
        F.sum("vuelos_retrasados").alias("retrasados"),
        F.sum("vuelos_cancelados").alias("cancelados"),
        F.sum("rutas_por_revisar").alias("rutas_por_revisar"),
        pct(F.sum("vuelos_a_tiempo"), F.sum("vuelos_operados")).alias("puntualidad_pct"),
    )
)
display(estados)

e = estados.first()
assert e["a_tiempo"] + e["retrasados"] == e["operados"]
assert e["operados"] + e["cancelados"] == e["total"]
assert e["rutas_por_revisar"] == 25

participacion = gold("kpi_aeronave").agg(F.sum("participacion_costo_pct")).first()[0]
assert abs(float(participacion) - 100) < 0.1, participacion
por_fab = gold("kpi_mantenimiento_fabricante").groupBy("fabricante").agg(F.sum("participacion_en_fabricante_pct").alias("p"))
assert por_fab.filter(F.abs(F.col("p") - 100) >= 0.1).count() == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.8 · Consulta, plan, archivos e historial
# MAGIC Ejecutamos la consulta y después leemos su plan con `explain("formatted")` (equivale a `EXPLAIN FORMATTED`). `DESCRIBE DETAIL` muestra `numFiles` y `sizeInBytes`; `DESCRIBE HISTORY` las versiones de la tabla Delta. Sobre la salida de la consulta: **See performance → See query profile**.

# COMMAND ----------

operados_por_pais = (
    gold("vuelos_enriquecidos")
    .filter("operado")
    .groupBy("origen_pais")
    .agg(F.count("*").alias("vuelos_operados"))
)
display(operados_por_pais)

# COMMAND ----------

operados_por_pais.explain("formatted")

# COMMAND ----------

display(spark.sql("DESCRIBE DETAIL aerolinea.gold.vuelos_enriquecidos"))

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY aerolinea.gold.vuelos_enriquecidos"))
