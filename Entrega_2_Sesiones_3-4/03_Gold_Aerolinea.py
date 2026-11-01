# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Tablas Gold e indicadores
# MAGIC Lee las tablas Silver persistentes y publica las seis tablas de `aerolinea.gold` de la guía: `vuelos_enriquecidos`, `mantenimientos_enriquecidos`, `kpi_puntualidad_vuelos`, `kpi_costo_mantenimiento`, `resumen_aeronave_mes` y `base_ml_mantenimiento`.
# MAGIC
# MAGIC Todas las celdas SQL de la guía están **traducidas a PySpark** (joins, `groupBy`/`agg`, `F.when` en lugar de `CASE WHEN`, `dividir()` = `/ NULLIF(x, 0)` para evitar dividir entre cero).

# COMMAND ----------

from pyspark.sql import functions as F


def publicar(df, tabla):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(tabla)


def contar_si(condicion):
    """SUM(CASE WHEN condicion THEN 1 ELSE 0 END)"""
    return F.sum(F.when(condicion, 1).otherwise(0))


def dividir(numerador, denominador):
    """numerador / NULLIF(denominador, 0): NULL cuando el denominador es 0 (sin error con ANSI)."""
    return F.when(denominador != 0, numerador / denominador)


vuelos_slv = spark.table("aerolinea.silver.vuelos")
aeronaves_slv = spark.table("aerolinea.silver.aeronaves")
aeropuertos_slv = spark.table("aerolinea.silver.aeropuertos")
mantenimientos_slv = spark.table("aerolinea.silver.mantenimientos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 · Vuelos enriquecidos
# MAGIC Una fila por vuelo. Se une `aeronaves` y dos copias de `aeropuertos` (origen y destino). `mes` es el primer día del mes; `operado` es verdadero para `a_tiempo` y `retrasado`.

# COMMAND ----------

v = vuelos_slv.alias("v")
a = aeronaves_slv.alias("a")
o = aeropuertos_slv.alias("o")
d = aeropuertos_slv.alias("d")

vuelos_enriquecidos = (
    v.join(a, F.col("v.aeronave_id") == F.col("a.aeronave_id"), "left")
    .join(o, F.col("v.origen_id") == F.col("o.aeropuerto_id"), "left")
    .join(d, F.col("v.destino_id") == F.col("d.aeropuerto_id"), "left")
    .select(
        "v.vuelo_id", "v.fecha",
        F.trunc("v.fecha", "month").alias("mes"),
        "v.aeronave_id", "a.modelo", "a.fabricante", "a.anio_fabricacion",
        (F.year("v.fecha") - F.col("a.anio_fabricacion")).alias("edad_aeronave"),
        "v.origen_id",
        F.col("o.nombre").alias("origen_nombre"),
        F.col("o.ciudad").alias("origen_ciudad"),
        F.col("o.pais").alias("origen_pais"),
        "v.destino_id",
        F.col("d.nombre").alias("destino_nombre"),
        F.col("d.ciudad").alias("destino_ciudad"),
        F.col("d.pais").alias("destino_pais"),
        "v.estado", "v.duracion_min",
        (F.col("v.estado") != "cancelado").alias("operado"),
        "v.ruta_por_revisar", "v.archivo_origen",
    )
)
publicar(vuelos_enriquecidos, "aerolinea.gold.vuelos_enriquecidos")

# COMMAND ----------

display(spark.table("aerolinea.gold.vuelos_enriquecidos").orderBy("vuelo_id").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 · Mantenimientos enriquecidos
# MAGIC La edad se calcula en el año de cada intervención. No se une cada intervención con todos los vuelos de su aeronave.

# COMMAND ----------

m = mantenimientos_slv.alias("m")

mantenimientos_enriquecidos = (
    m.join(a, F.col("m.aeronave_id") == F.col("a.aeronave_id"), "left")
    .select(
        "m.mantenimiento_id", "m.fecha",
        F.trunc("m.fecha", "month").alias("mes"),
        "m.aeronave_id", "a.modelo", "a.fabricante", "a.anio_fabricacion",
        (F.year("m.fecha") - F.col("a.anio_fabricacion")).alias("edad_aeronave"),
        "m.tipo", "m.costo_usd", "m.duracion_hr", "m.archivo_origen",
    )
)
publicar(mantenimientos_enriquecidos, "aerolinea.gold.mantenimientos_enriquecidos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 · Comprobación del enriquecimiento
# MAGIC Cien vuelos y cien mantenimientos, igual que en Silver: cada dimensión aporta una sola coincidencia por llave.

# COMMAND ----------

vuelos = spark.table("aerolinea.gold.vuelos_enriquecidos").count()
mantenimientos = spark.table("aerolinea.gold.mantenimientos_enriquecidos").count()
assert vuelos == spark.table("aerolinea.silver.vuelos").count(), "La unión multiplicó o perdió vuelos."
assert mantenimientos == spark.table("aerolinea.silver.mantenimientos").count(), "La unión multiplicó o perdió intervenciones."
print("Vuelos enriquecidos:", vuelos)
print("Mantenimientos enriquecidos:", mantenimientos)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 · Puntualidad por grupo
# MAGIC Puntualidad = `a_tiempo / operados`; cancelación = `cancelados / total`. `dividir()` deja NULL si un grupo no tiene operados. La duración media excluye cancelaciones.

# COMMAND ----------

ve = spark.table("aerolinea.gold.vuelos_enriquecidos")

kpi_puntualidad_vuelos = (
    ve.groupBy("modelo", "fabricante", "origen_pais", "mes")
    .agg(
        F.count("*").alias("total_vuelos"),
        contar_si(F.col("operado")).alias("vuelos_operados"),
        contar_si(F.col("estado") == "a_tiempo").alias("vuelos_a_tiempo"),
        contar_si(F.col("estado") == "retrasado").alias("vuelos_retrasados"),
        contar_si(F.col("estado") == "cancelado").alias("vuelos_cancelados"),
        F.round(dividir(100.0 * contar_si(F.col("estado") == "a_tiempo"),
                        contar_si(F.col("operado"))), 2).alias("puntualidad_operados_pct"),
        F.round(100.0 * contar_si(F.col("estado") == "cancelado") / F.count("*"), 2).alias("cancelacion_pct"),
        F.round(F.avg(F.when(F.col("operado"), F.col("duracion_min"))), 2).alias("duracion_promedio_operados_min"),
        contar_si(F.col("ruta_por_revisar")).alias("rutas_por_revisar"),
    )
)
publicar(kpi_puntualidad_vuelos, "aerolinea.gold.kpi_puntualidad_vuelos")

# COMMAND ----------

display(spark.table("aerolinea.gold.kpi_puntualidad_vuelos").orderBy("modelo", "origen_pais"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 · Puntualidad global
# MAGIC Sumamos los conteos de los grupos y volvemos a dividir (no se promedian porcentajes). Esperado: 100 registros, 69 operados, 40 a tiempo, 31 cancelados, 57,97 % de puntualidad y 31 % de cancelación.

# COMMAND ----------

global_vuelos = (
    spark.table("aerolinea.gold.kpi_puntualidad_vuelos")
    .agg(
        F.sum("total_vuelos").alias("vuelos"),
        F.sum("vuelos_operados").alias("operados"),
        F.sum("vuelos_a_tiempo").alias("a_tiempo"),
        F.sum("vuelos_cancelados").alias("cancelados"),
        F.round(dividir(100.0 * F.sum("vuelos_a_tiempo"), F.sum("vuelos_operados")), 2).alias("puntualidad_pct"),
        F.round(100.0 * F.sum("vuelos_cancelados") / F.sum("total_vuelos"), 2).alias("cancelacion_pct"),
    )
)
display(global_vuelos)

g = global_vuelos.first()
assert (g["vuelos"], g["operados"], g["a_tiempo"], g["cancelados"]) == (100, 69, 40, 31), g
assert float(g["puntualidad_pct"]) == 57.97 and float(g["cancelacion_pct"]) == 31.0, g

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.6 · Costos de mantenimiento
# MAGIC Una fila por aeronave, tipo y mes. El total de las cien intervenciones se conserva al agrupar.

# COMMAND ----------

me = spark.table("aerolinea.gold.mantenimientos_enriquecidos")

kpi_costo_mantenimiento = (
    me.groupBy("aeronave_id", "modelo", "fabricante", "tipo", "mes")
    .agg(
        F.count("*").alias("total_mantenimientos"),
        F.sum("costo_usd").alias("costo_total_usd"),
        F.round(F.avg("costo_usd"), 2).alias("costo_promedio_usd"),
        F.sum("duracion_hr").alias("duracion_total_hr"),
        F.round(F.avg("duracion_hr"), 2).alias("duracion_promedio_hr"),
    )
)
publicar(kpi_costo_mantenimiento, "aerolinea.gold.kpi_costo_mantenimiento")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.7 · Costo por tipo (para el gráfico)
# MAGIC Sobre la salida: **+ → Visualization → Bar**, `tipo` en el eje X y `costo_total_usd` en el Y. Nombre de la pestaña: *Costo por tipo · mayo 2024 (USD)*.

# COMMAND ----------

costo_por_tipo = (
    spark.table("aerolinea.gold.kpi_costo_mantenimiento")
    .groupBy("tipo")
    .agg(
        F.sum("total_mantenimientos").alias("intervenciones"),
        F.sum("costo_total_usd").alias("costo_total_usd"),
    )
    .orderBy(F.desc("costo_total_usd"))
)
display(costo_por_tipo)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.8 · Actividad y costo en el mismo nivel de detalle
# MAGIC Primero se resume cada fuente por aeronave y mes; después se unen con `full_outer`. `F.coalesce` muestra cero cuando la otra fuente no tiene eventos ese mes.

# COMMAND ----------

actividad = (
    ve.groupBy("aeronave_id", "mes")
    .agg(F.count("*").alias("total_vuelos"),
         contar_si(F.col("operado")).alias("vuelos_operados"))
    .alias("act")
)
mantenimiento = (
    me.groupBy("aeronave_id", "mes")
    .agg(F.count("*").alias("total_mantenimientos"),
         F.sum("costo_usd").alias("costo_total_usd"))
    .alias("mto")
)

resumen_aeronave_mes = (
    actividad.join(
        mantenimiento,
        (F.col("act.aeronave_id") == F.col("mto.aeronave_id")) & (F.col("act.mes") == F.col("mto.mes")),
        "full_outer",
    )
    .select(
        F.coalesce("act.aeronave_id", "mto.aeronave_id").alias("aeronave_id"),
        F.coalesce("act.mes", "mto.mes").alias("mes"),
        F.coalesce("act.total_vuelos", F.lit(0)).alias("total_vuelos"),
        F.coalesce("act.vuelos_operados", F.lit(0)).alias("vuelos_operados"),
        F.coalesce("mto.total_mantenimientos", F.lit(0)).alias("total_mantenimientos"),
        F.coalesce("mto.costo_total_usd", F.lit(0)).alias("costo_total_usd"),
    )
)
publicar(resumen_aeronave_mes, "aerolinea.gold.resumen_aeronave_mes")

# COMMAND ----------

display(spark.table("aerolinea.gold.resumen_aeronave_mes").orderBy("aeronave_id", "mes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.9 · Base detallada para la sesión 5
# MAGIC `costo_usd` será el objetivo. La duración final y el costo por hora no se incluyen como predictores (no se conocen al estimar el costo).

# COMMAND ----------

base_ml_mantenimiento = me.select(
    "mantenimiento_id", "fecha", "aeronave_id",
    "tipo", "modelo", "fabricante", "edad_aeronave", "costo_usd",
)
publicar(base_ml_mantenimiento, "aerolinea.gold.base_ml_mantenimiento")

# COMMAND ----------

display(spark.table("aerolinea.gold.base_ml_mantenimiento").orderBy("mantenimiento_id").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comprobación de Gold
# MAGIC Esperamos 100 vuelos enriquecidos, 100 mantenimientos enriquecidos, 20 grupos de puntualidad, 30 de mantenimiento, 10 resúmenes mensuales y 100 filas para ML.

# COMMAND ----------

esperados = {
    "vuelos_enriquecidos": 100, "mantenimientos_enriquecidos": 100,
    "kpi_puntualidad_vuelos": 20, "kpi_costo_mantenimiento": 30,
    "resumen_aeronave_mes": 10, "base_ml_mantenimiento": 100,
}
obtenidos = {t: spark.table(f"aerolinea.gold.{t}").count() for t in esperados}
print(obtenidos)
assert obtenidos == esperados, f"Conteos Gold inesperados: {obtenidos}"
