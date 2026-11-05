"""Tarea Silver_Transform del Job Trabajo_Final_Vuelos.

Lee aerolinea_tf.bronze, une aerolíneas y aeropuertos de origen/destino, calcula
delay_total, on_time, distance_km y delay_category, valida y publica en aerolinea_tf.silver:
flights_enriched, airline_metrics, airline_origin_ontime y airport_metrics.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#=======================
# Configuración
#=======================

CATALOG = "aerolinea_tf"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
MILLA_KM = 1.609344

spark = SparkSession.builder.getOrCreate()

def guardar(df, tabla, particion=None):
    escritor = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if particion:
        escritor = escritor.partitionBy(particion)
    escritor.saveAsTable(f"{SILVER}.{tabla}")

def pct_a_tiempo():
    return F.round(F.avg(F.col("on_time").cast("int")) * 100, 2)

#==========================
# 1. Leer Bronze
#==========================

flights = spark.table(f"{BRONZE}.flights").drop("source_file", "ingest_timestamp")
airlines = spark.table(f"{BRONZE}.airlines")
airports = spark.table(f"{BRONZE}.airports")

# Claves únicas en las dimensiones: la unión no puede multiplicar vuelos
assert airlines.select("airline_id").distinct().count() == airlines.count(), "airline_id repetido"
assert airports.select("airport_id").distinct().count() == airports.count(), "airport_id repetido"

#==========================
# 2. Unir aerolíneas y aeropuertos
#==========================

origen = airports.select(F.col("airport_id").alias("origin_airport"),
                         F.col("city").alias("origin_city"), F.col("country").alias("origin_country"))
destino = airports.select(F.col("airport_id").alias("destination_airport"),
                          F.col("city").alias("destination_city"), F.col("country").alias("destination_country"))

flights_joined = (
    flights
    .join(airlines.select("airline_id", "airline_name"), "airline_id", "left")
    .join(origen, "origin_airport", "left")
    .join(destino, "destination_airport", "left")
)

#==========================
# 3. Nuevas columnas, validación y limpieza
#==========================

flights_silver = (
    flights_joined
    .withColumn("delay_total", F.col("departure_delay") + F.col("arrival_delay"))
    .withColumn("on_time", F.when(F.col("arrival_delay").isNotNull(), F.col("arrival_delay") <= 15))
    .withColumn("distance_km", F.round(F.col("distance_miles") * MILLA_KM, 2))
    .withColumn(
        "delay_category",
        F.when(F.col("arrival_delay").isNull(), "Unknown")
         .when(F.col("arrival_delay") <= 0, "None")
         .when(F.col("arrival_delay") <= 15, "Short")
         .when(F.col("arrival_delay") <= 45, "Medium")
         .otherwise("Long"),
    )
    # Textos de referencia: se rellenan. Retrasos: se conservan NULL (no se inventa el dato).
    .fillna("Desconocido", subset=["airline_name", "origin_city", "origin_country",
                                   "destination_city", "destination_country"])
    .withColumn("delay_data_complete",
                F.col("departure_delay").isNotNull() & F.col("arrival_delay").isNotNull())
    .dropDuplicates(["flight_id"])
    .withColumn("silver_timestamp", F.current_timestamp())
    .select("flight_id", "flight_date", "airline_id", "airline_name",
            "origin_airport", "origin_city", "origin_country",
            "destination_airport", "destination_city", "destination_country",
            "departure_delay", "arrival_delay", "delay_total", "on_time", "delay_category",
            "distance_miles", "distance_km", "delay_data_complete", "silver_timestamp")
)

controles = flights_silver.agg(
    F.count("*").alias("vuelos"),
    F.countDistinct("flight_id").alias("ids_unicos"),
    F.sum(F.col("flight_date").isNull().cast("int")).alias("sin_fecha"),
    F.sum((F.col("airline_name") == "Desconocido").cast("int")).alias("aerolinea_desconocida"),
    F.sum((F.col("origin_city") == "Desconocido").cast("int")).alias("origen_desconocido"),
    F.sum((~F.col("delay_data_complete")).cast("int")).alias("sin_retraso_completo"),
).first()
print("Controles Silver:", controles.asDict())
assert controles["vuelos"] == controles["ids_unicos"] == flights.count(), "Se perdieron o duplicaron vuelos"
assert controles["sin_fecha"] == 0, "Hay vuelos sin fecha"

#==========================
# 4. Agregaciones
#==========================

airline_metrics = (
    flights_silver.groupBy("airline_id", "airline_name")
    .agg(
        F.count("*").alias("num_flights"),
        F.round(F.avg("arrival_delay"), 2).alias("avg_arrival_delay"),
        F.round(F.avg("departure_delay"), 2).alias("avg_departure_delay"),
        F.round(F.avg("delay_total"), 2).alias("avg_delay_total"),
        F.round(F.sum("distance_km"), 2).alias("total_distance_km"),
        pct_a_tiempo().alias("on_time_pct"),
    )
)

airline_origin_ontime = (
    flights_silver.groupBy("airline_id", "airline_name", "origin_airport", "origin_city")
    .agg(F.count("*").alias("num_flights"), pct_a_tiempo().alias("on_time_pct"))
)

salientes = (
    flights_silver.groupBy(F.col("origin_airport").alias("airport_id"))
    .agg(F.count("*").alias("outgoing_flights"),
         F.round(F.avg("departure_delay"), 2).alias("avg_delay_as_origin"),
         pct_a_tiempo().alias("on_time_pct_outgoing"))
)
entrantes = (
    flights_silver.groupBy(F.col("destination_airport").alias("airport_id"))
    .agg(F.count("*").alias("incoming_flights"),
         F.round(F.avg("arrival_delay"), 2).alias("avg_delay_as_destination"),
         pct_a_tiempo().alias("on_time_pct_incoming"))
)
airport_metrics = (
    salientes.join(entrantes, "airport_id", "full_outer")
    .join(airports.select("airport_id", "city", "country"), "airport_id", "left")
    .select("airport_id", "city", "country", "outgoing_flights", "incoming_flights",
            "avg_delay_as_origin", "avg_delay_as_destination",
            "on_time_pct_outgoing", "on_time_pct_incoming")
)

#==========================
# 5. Escribir en Silver (Delta)
#==========================

guardar(flights_silver.withColumn("flight_month", F.date_format("flight_date", "yyyy-MM")),
        "flights_enriched", particion="flight_month")
guardar(airline_metrics, "airline_metrics")
guardar(airline_origin_ontime, "airline_origin_ontime")
guardar(airport_metrics, "airport_metrics")

#==========================
# 6. Verificación
#==========================

conteos = {t: spark.table(f"{SILVER}.{t}").count()
           for t in ["flights_enriched", "airline_metrics", "airline_origin_ontime", "airport_metrics"]}
print("Conteos Silver:", conteos)
suma = spark.table(f"{SILVER}.airline_metrics").agg(F.sum("num_flights")).first()[0]
assert suma == conteos["flights_enriched"], "airline_metrics no reconcilia con flights_enriched"

print("Silver_Transform completada")
