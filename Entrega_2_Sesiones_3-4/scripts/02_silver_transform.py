from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# =========================
# Configuración
# =========================
CATALOG = "airline_mantenimiento"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

spark = SparkSession.builder.getOrCreate()

# =========================
# 0. Asegurar schema Silver
# =========================

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

# =========================
# 1. Leer Bronze
# =========================

vuelos_bz = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.vuelos")
aeronaves_bz = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.aeronaves")
aeropuertos_bz = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.aeropuertos")
mantenimientos_bz = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.mantenimientos")

# =========================
# 2. Silver vuelos
# =========================
#Transformaciones Silver vuelos
vuelos_slv = (
    vuelos_bz
    .select("vuelo_id",
            "fecha",
            "origen_id",
            "destino_id",
            "aeronave_id",
            "estado",
            "duracion_min"
    )
    .withColumn("vuelo_id", F.upper(F.trim(F.col("vuelo_id"))))
    .withColumn("origen_id", F.upper(F.trim(F.col("origen_id"))))
    .withColumn("destino_id", F.upper(F.trim(F.col("destino_id"))))
    .withColumn("aeronave_id", F.upper(F.trim(F.col("aeronave_id"))))
    .withColumn("estado", F.lower(F.trim(F.col("estado"))))
    .withColumn("fecha", F.to_date(F.col("fecha")))
    .withColumn("duracion_min", F.col("duracion_min").cast("int"))
    .withColumn("ruta_por_revisar", F.col("origen_id") == F.col("destino_id"))
    .dropDuplicates(["vuelo_id"])
)

#Trasformaciones Silver aeronaves
aeronaves_slv = (
    aeronaves_bz
    .select("aeronave_id",
            "modelo",
            "fabricante",
            "anio_fabricacion"
    )
    .withColumn("aeronave_id", F.upper(F.trim(F.col("aeronave_id"))))
    .withColumn("anio_fabricacion", F.col("anio_fabricacion").cast("int"))
    .dropDuplicates(["aeronave_id"])
)

#Trasformaciones Silver aeropuertos
aeropuertos_slv = (
    aeropuertos_bz
    .select("aeropuerto_id",
            "nombre",
            "ciudad",
            "pais",
            "lat",
            "lon"
    )
    .withColumn("aeropuerto_id", F.upper(F.trim(F.col("aeropuerto_id"))))
    .withColumn("lat", F.col("lat").cast("double"))
    .withColumn("lon", F.col("lon").cast("double"))
    .dropDuplicates(["aeropuerto_id"])
)

#Trasformaciones Silver mantenimientos
mantenimientos_slv = (
    mantenimientos_bz
    .select("mantenimiento_id",
            "aeronave_id",
            "fecha",
            "tipo",
            "costo_usd",
            "duracion_hr"
    )
    .withColumn("mantenimiento_id", F.upper(F.trim(F.col("mantenimiento_id"))))
    .withColumn("aeronave_id", F.upper(F.trim(F.col("aeronave_id"))))
    .withColumn("tipo", F.lower(F.trim(F.col("tipo"))))
    .withColumn("fecha", F.to_date(F.col("fecha")))
    .withColumn("costo_usd", F.col("costo_usd").cast("decimal(14,2)"))
    .withColumn("duracion_hr", F.col("duracion_hr").cast("double"))
    .dropDuplicates(["mantenimiento_id"])
)

# =========================
# 3. Validaciónes de Calidad
# =========================
nulos_vuelos = vuelos_slv.filter(F.col("vuelo_id").isNull()).count()
duplicados_vuelos = vuelos_slv.groupBy("vuelo_id").count().filter(F.col("count") > 1).count()
estados_invalidos = vuelos_slv.filter(~F.col("estado").isin("a_tiempo", "retrasado", "cancelado")).count()
vuelos_sin_aeronave = vuelos_slv.join(aeronaves_slv, "aeronave_id", "left_anti").count()
mantenimientos_sin_aeronave = mantenimientos_slv.join(aeronaves_slv, "aeronave_id", "left_anti").count()
costos_invalidos = mantenimientos_slv.filter(F.col("costo_usd").isNull() | (F.col("costo_usd") < 0)).count()

print("Nulos vuelo_id:", nulos_vuelos)
print("Duplicados vuelos:", duplicados_vuelos)
print("Estados inválidos:", estados_invalidos)
print("Vuelos sin aeronave:", vuelos_sin_aeronave)
print("Mantenimientos sin aeronave:", mantenimientos_sin_aeronave)
print("Costos inválidos:", costos_invalidos)
print("Rutas por revisar:", vuelos_slv.filter("ruta_por_revisar").count())

assert nulos_vuelos == 0 and duplicados_vuelos == 0 and estados_invalidos == 0, "Revisar vuelos antes de publicar Silver"
assert vuelos_sin_aeronave == 0 and mantenimientos_sin_aeronave == 0, "Hay referencias a aeronaves inexistentes"
assert costos_invalidos == 0, "Hay costos nulos o negativos"

# =========================
# 4. Guardar en Silver
# =========================

vuelos_slv.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.vuelos")

aeronaves_slv.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.aeronaves")

aeropuertos_slv.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.aeropuertos")

mantenimientos_slv.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.mantenimientos")

print("Silver Delta completada")
print(f"Tablas creadas/actualizadas: {CATALOG}.{SILVER_SCHEMA}.vuelos, aeronaves, aeropuertos, mantenimientos")
