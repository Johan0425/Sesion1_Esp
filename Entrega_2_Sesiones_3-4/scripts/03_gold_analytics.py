# 03_gold_analytics.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =========================
# Configuración
# =========================
CATALOG = "airline_mantenimiento"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

spark = SparkSession.builder.getOrCreate()


def guardar(df, tabla):
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", True) \
        .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.{tabla}")


def contar_si(condicion):
    return F.sum(F.when(condicion, 1).otherwise(0))


def dividir(numerador, denominador):
    """numerador / NULLIF(denominador, 0): NULL cuando el denominador es 0 (sin error con ANSI)."""
    return F.when(denominador != 0, numerador / denominador)


# =========================
# 0. Asegurar schema Gold
# =========================
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")

# =========================
# 1. Leer Silver
# =========================
vuelos_slv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.vuelos")
aeronaves_slv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.aeronaves")
aeropuertos_slv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.aeropuertos")
mantenimientos_slv = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.mantenimientos")

# =========================
# 2. Vuelos enriquecidos (una fila por vuelo)
# =========================
origen = aeropuertos_slv.select(
    F.col("aeropuerto_id").alias("origen_id"),
    F.col("ciudad").alias("origen_ciudad"),
    F.col("pais").alias("origen_pais"),
)
destino = aeropuertos_slv.select(
    F.col("aeropuerto_id").alias("destino_id"),
    F.col("ciudad").alias("destino_ciudad"),
    F.col("pais").alias("destino_pais"),
)

vuelos_gold = (
    vuelos_slv
    .join(aeronaves_slv, "aeronave_id", "left")
    .join(origen, "origen_id", "left")
    .join(destino, "destino_id", "left")
    .withColumn("mes", F.trunc("fecha", "month"))
    .withColumn("edad_aeronave", F.year("fecha") - F.col("anio_fabricacion"))
    .withColumn("operado", F.col("estado") != "cancelado")
)
guardar(vuelos_gold, "vuelos_enriquecidos")

# =========================
# 3. Mantenimientos enriquecidos
# =========================
mantenimientos_gold = (
    mantenimientos_slv
    .join(aeronaves_slv, "aeronave_id", "left")
    .withColumn("mes", F.trunc("fecha", "month"))
    .withColumn("edad_aeronave", F.year("fecha") - F.col("anio_fabricacion"))
)
guardar(mantenimientos_gold, "mantenimientos_enriquecidos")

# =========================
# 4. KPI puntualidad por modelo y país de origen
# =========================
kpi_puntualidad = (
    vuelos_gold
    .groupBy("modelo", "fabricante", "origen_pais", "mes")
    .agg(
        F.count("*").alias("total_vuelos"),
        contar_si(F.col("operado")).alias("vuelos_operados"),
        contar_si(F.col("estado") == "a_tiempo").alias("vuelos_a_tiempo"),
        contar_si(F.col("estado") == "retrasado").alias("vuelos_retrasados"),
        contar_si(F.col("estado") == "cancelado").alias("vuelos_cancelados"),
        F.round(F.avg(F.when(F.col("operado"), F.col("duracion_min"))), 2).alias("duracion_promedio_operados_min"),
    )
    .withColumn("puntualidad_operados_pct",
                F.round(dividir(100.0 * F.col("vuelos_a_tiempo"), F.col("vuelos_operados")), 2))
    .withColumn("cancelacion_pct",
                F.round(100.0 * F.col("vuelos_cancelados") / F.col("total_vuelos"), 2))
)
guardar(kpi_puntualidad, "kpi_puntualidad_vuelos")

# =========================
# 5. KPI costo de mantenimiento por aeronave y tipo
# =========================
kpi_costo = (
    mantenimientos_gold
    .groupBy("aeronave_id", "modelo", "fabricante", "tipo", "mes")
    .agg(
        F.count("*").alias("total_mantenimientos"),
        F.sum("costo_usd").alias("costo_total_usd"),
        F.round(F.avg("costo_usd"), 2).alias("costo_promedio_usd"),
        F.round(F.sum("duracion_hr"), 2).alias("duracion_total_hr"),
    )
)
guardar(kpi_costo, "kpi_costo_mantenimiento")

# =========================
# 6. Ranking de aeronaves por costo (Window)
# =========================
vuelos_por_aeronave = vuelos_gold.groupBy("aeronave_id").agg(
    F.count("*").alias("total_vuelos"),
    contar_si(F.col("operado")).alias("vuelos_operados"),
)

ranking_costo = (
    mantenimientos_gold
    .groupBy("aeronave_id", "modelo", "fabricante")
    .agg(
        F.count("*").alias("total_mantenimientos"),
        F.sum("costo_usd").alias("costo_total_usd"),
    )
    .join(vuelos_por_aeronave, "aeronave_id", "left")
    .fillna(0, subset=["total_vuelos", "vuelos_operados"])
    .withColumn("costo_por_vuelo_operado_usd",
                F.round(dividir(F.col("costo_total_usd"), F.col("vuelos_operados")), 2))
    .withColumn("ranking_costo", F.dense_rank().over(Window.orderBy(F.desc("costo_total_usd"))))
    .withColumn("ranking_en_fabricante",
                F.dense_rank().over(Window.partitionBy("fabricante").orderBy(F.desc("costo_total_usd"))))
)
guardar(ranking_costo, "ranking_costo_aeronave")

# =========================
# 7. Verificación
# =========================
total_vuelos = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.kpi_puntualidad_vuelos").agg(F.sum("total_vuelos")).first()[0]
costo_silver = mantenimientos_slv.agg(F.sum("costo_usd")).first()[0]
costo_gold = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.kpi_costo_mantenimiento").agg(F.sum("costo_total_usd")).first()[0]
costo_ranking = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.ranking_costo_aeronave").agg(F.sum("costo_total_usd")).first()[0]

print("Vuelos en KPI:", total_vuelos)
print("Costo Silver:", costo_silver, "| Costo Gold:", costo_gold, "| Costo ranking:", costo_ranking)
assert total_vuelos == vuelos_slv.count(), "El KPI de puntualidad no explica todos los vuelos"
assert costo_silver == costo_gold == costo_ranking, "El costo Gold no coincide con Silver"

spark.table(f"{CATALOG}.{GOLD_SCHEMA}.ranking_costo_aeronave").orderBy("ranking_costo").show(truncate=False)

print("Gold analytics completada")
print(f"Tablas creadas: {CATALOG}.{GOLD_SCHEMA}.vuelos_enriquecidos, mantenimientos_enriquecidos, "
      "kpi_puntualidad_vuelos, kpi_costo_mantenimiento, ranking_costo_aeronave")
