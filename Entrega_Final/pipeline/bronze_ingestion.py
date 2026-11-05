"""Tarea Bronze_Ingestion del Job Trabajo_Final_Vuelos.

Lee los tres CSV del volumen /Volumes/aerolinea_tf/fuentes/landing, convierte tipos,
descarta filas sin flight_id, airline_id u origin_airport, añade ingest_timestamp
y publica aerolinea_tf.bronze.flights, airlines y airports en Delta.
"""
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#=======================
# Configuración
#=======================

CATALOG = "aerolinea_tf"
BRONZE = f"{CATALOG}.bronze"
LANDING = f"/Volumes/{CATALOG}/fuentes/landing"
ARCHIVOS = ["flights.csv", "airlines.csv", "airports.csv"]

SCHEMAS = {
    "fuentes": "Archivos de origen sin modificar (volumen landing)",
    "bronze": "Apartado 1: datos ingeridos, tipados y con ingest_timestamp",
    "silver": "Apartado 2: vuelos enriquecidos y métricas por aerolínea y aeropuerto",
    "analisis": "Apartado 3: análisis temporal y rankings con funciones de ventana",
    "gold": "Apartado 4: KPI de negocio listos para dashboards y base de ML",
    "ml": "Apartado 6: modelo, métricas y predicciones del retraso de llegada",
}

spark = SparkSession.builder.getOrCreate()

#==========================
# 0. Catálogo, esquemas y volumen
#==========================

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema, comentario in SCHEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{comentario}'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.fuentes.landing")

#==========================
# 1. CSV en el volumen
#==========================

# Si el repositorio está clonado como Git folder, se copian al volumen los CSV que falten
# desde la carpeta datos/ de la entrega (esta tarea está en pipeline/).
try:
    datos_repo = Path(__file__).resolve().parent.parent / "datos"
except NameError:
    datos_repo = None

for archivo in ARCHIVOS:
    destino = Path(LANDING) / archivo
    if not destino.exists() and datos_repo and (datos_repo / archivo).is_file():
        shutil.copyfile(datos_repo / archivo, destino)
        print("Copiado desde el repositorio:", archivo)

faltantes = [a for a in ARCHIVOS if not (Path(LANDING) / a).exists()]
assert not faltantes, f"Subir al volumen {LANDING} los archivos: {faltantes}"

#==========================
# 2. Leer CSV (todo como texto, FAILFAST)
#==========================

def leer_csv(archivo, esquema):
    return spark.read.csv(f"{LANDING}/{archivo}", header=True, schema=esquema,
                          mode="FAILFAST", enforceSchema=False)

flights_raw = leer_csv(
    "flights.csv",
    """flight_id STRING, flight_date STRING, airline_id STRING, origin_airport STRING,
       destination_airport STRING, departure_delay STRING, arrival_delay STRING,
       distance_miles STRING""",
)
airlines_raw = leer_csv("airlines.csv", "airline_id STRING, airline_name STRING")
airports_raw = leer_csv("airports.csv", "airport_id STRING, city STRING, country STRING")

#==========================
# 3. Tipos, nulos obligatorios e ingest_timestamp
#==========================

def codigo(c):
    return F.when(F.trim(c) != "", F.upper(F.trim(c)))

def texto(c):
    return F.when(F.trim(c) != "", F.trim(c))

def entero_desde_decimal(c):
    # los retrasos llegan como "25.0"
    return F.expr(f"try_cast(try_cast({c} AS DOUBLE) AS INT)")

flights_bronze = (
    flights_raw.select(
        codigo("flight_id").alias("flight_id"),
        F.expr("try_cast(flight_date AS DATE)").alias("flight_date"),
        codigo("airline_id").alias("airline_id"),
        codigo("origin_airport").alias("origin_airport"),
        codigo("destination_airport").alias("destination_airport"),
        entero_desde_decimal("departure_delay").alias("departure_delay"),
        entero_desde_decimal("arrival_delay").alias("arrival_delay"),
        F.expr("try_cast(distance_miles AS INT)").alias("distance_miles"),
    )
    .dropna(subset=["flight_id", "airline_id", "origin_airport"])
    .withColumn("source_file", F.lit("flights.csv"))
    .withColumn("ingest_timestamp", F.current_timestamp())
)

airlines_bronze = (
    airlines_raw.select(codigo("airline_id").alias("airline_id"), texto("airline_name").alias("airline_name"))
    .dropna(subset=["airline_id"])
    .withColumn("source_file", F.lit("airlines.csv"))
    .withColumn("ingest_timestamp", F.current_timestamp())
)

airports_bronze = (
    airports_raw.select(codigo("airport_id").alias("airport_id"), texto("city").alias("city"),
                        texto("country").alias("country"))
    .dropna(subset=["airport_id"])
    .withColumn("source_file", F.lit("airports.csv"))
    .withColumn("ingest_timestamp", F.current_timestamp())
)

#==========================
# 4. Escribir en Bronze (Delta)
#==========================

for df, tabla in [(flights_bronze, "flights"), (airlines_bronze, "airlines"), (airports_bronze, "airports")]:
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{BRONZE}.{tabla}"))

#==========================
# 5. Verificación
#==========================

leidos = flights_raw.count()
conteos = {t: spark.table(f"{BRONZE}.{t}").count() for t in ["flights", "airlines", "airports"]}
print(f"Vuelos leídos: {leidos} · descartados por nulos obligatorios: {leidos - conteos['flights']}")
print("Conteos Bronze:", conteos)
assert conteos["flights"] > 0 and conteos["airlines"] > 0 and conteos["airports"] > 0, conteos
assert spark.table(f"{BRONZE}.flights").filter("flight_date IS NULL").count() == 0, "Fechas no válidas"

print("Bronze_Ingestion completada")
