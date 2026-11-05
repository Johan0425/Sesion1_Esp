"""Tarea Gold_Analytics del Job Trabajo_Final_Vuelos.

Lee aerolinea_tf.silver.flights_enriched, publica los KPI de negocio en aerolinea_tf.gold,
aplica OPTIMIZE / ZORDER BY y reconcilia los totales con Silver.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

#=======================
# Configuración
#=======================

CATALOG = "aerolinea_tf"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

spark = SparkSession.builder.getOrCreate()

def guardar(df, tabla):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{GOLD}.{tabla}"))

def pct_a_tiempo():
    return F.round(F.avg(F.col("on_time").cast("int")) * 100, 2)

#==========================
# 1. Leer Silver y añadir columnas de calendario
#==========================

silver = (
    spark.table(f"{SILVER}.flights_enriched")
    .withColumn("month", F.month("flight_date"))
    .withColumn("day_of_week", ((F.dayofweek("flight_date") + 5) % 7) + 1)  # lunes = 1
)

#==========================
# 2. Tablas Gold
#==========================

# Retraso promedio por aerolínea y aeropuerto de origen, por mes
delay_airline_airport_month = (
    silver.groupBy("airline_id", "airline_name", F.col("origin_airport").alias("airport_id"),
                   F.col("origin_city").alias("city"), F.col("flight_month").alias("month"))
    .agg(F.count("*").alias("num_flights"),
         F.round(F.avg("departure_delay"), 2).alias("avg_departure_delay"),
         F.round(F.avg("arrival_delay"), 2).alias("avg_arrival_delay"),
         F.round(F.avg("delay_total"), 2).alias("avg_delay_total"),
         pct_a_tiempo().alias("on_time_pct"))
)

# Ranking de aeropuertos más puntuales (punctuality_rank = 1 es el mejor)
airport_punctuality_ranking = (
    silver.groupBy(F.col("origin_airport").alias("airport_id"), F.col("origin_city").alias("city"),
                   F.col("origin_country").alias("country"))
    .agg(F.count("*").alias("outgoing_flights"),
         F.sum(F.col("on_time").cast("int")).alias("on_time_flights"),
         pct_a_tiempo().alias("on_time_pct"),
         F.round(F.avg("arrival_delay"), 2).alias("avg_arrival_delay"))
    .withColumn("punctuality_rank", F.dense_rank().over(Window.orderBy(F.desc("on_time_pct"))))
)

# KPI de porcentaje de vuelos a tiempo por aerolínea
red = Window.partitionBy()
airline_ontime_kpi = (
    silver.groupBy("airline_id", "airline_name")
    .agg(F.count("*").alias("num_flights"),
         F.sum(F.col("on_time").cast("int")).alias("on_time_flights"),
         F.sum(F.col("on_time").isNotNull().cast("int")).alias("flights_with_arrival_data"),
         F.round(F.avg("arrival_delay"), 2).alias("avg_arrival_delay"),
         F.round(F.sum("distance_km"), 2).alias("total_distance_km"))
    .withColumn("on_time_pct", F.round(F.col("on_time_flights") * 100 / F.col("flights_with_arrival_data"), 2))
    .withColumn("network_on_time_pct", F.round(F.sum("on_time_flights").over(red) * 100
                                               / F.sum("flights_with_arrival_data").over(red), 2))
    .withColumn("diff_vs_network_pp", F.round(F.col("on_time_pct") - F.col("network_on_time_pct"), 2))
    .withColumn("ontime_rank", F.dense_rank().over(Window.orderBy(F.desc("on_time_pct"))))
)

# Base por vuelo para el apartado 6 (solo vuelos con ambos retrasos conocidos)
ml_flights = (
    silver.filter("delay_data_complete")
    .select("flight_id", "flight_date", "flight_month", "month", "day_of_week",
            "airline_id", "origin_airport", "destination_airport", "distance_km",
            "departure_delay", "arrival_delay")
)

guardar(delay_airline_airport_month, "delay_airline_airport_month")
guardar(airport_punctuality_ranking, "airport_punctuality_ranking")
guardar(airline_ontime_kpi, "airline_ontime_kpi")
guardar(ml_flights, "ml_flights")

#==========================
# 3. Optimización Delta
#==========================

optimizaciones = {
    f"{GOLD}.delay_airline_airport_month": "airline_id, airport_id",
    f"{GOLD}.ml_flights": "flight_date, airline_id",
    f"{SILVER}.flights_enriched": "airline_id, origin_airport",
    f"{GOLD}.airport_punctuality_ranking": None,
    f"{GOLD}.airline_ontime_kpi": None,
}
for tabla, zorder in optimizaciones.items():
    sentencia = f"OPTIMIZE {tabla}" + (f" ZORDER BY ({zorder})" if zorder else "")
    print(sentencia)
    spark.sql(sentencia)

#==========================
# 4. Reconciliación con Silver
#==========================

total_silver = silver.count()
puntuales_silver = silver.filter("on_time").count()

def suma(tabla, columna):
    return spark.table(f"{GOLD}.{tabla}").agg(F.sum(columna)).first()[0]

reglas = {
    "delay_airline_airport_month suma los vuelos de Silver":
        suma("delay_airline_airport_month", "num_flights") == total_silver,
    "airport_punctuality_ranking suma los vuelos de Silver":
        suma("airport_punctuality_ranking", "outgoing_flights") == total_silver,
    "airline_ontime_kpi suma los vuelos de Silver":
        suma("airline_ontime_kpi", "num_flights") == total_silver,
    "airline_ontime_kpi suma los vuelos puntuales de Silver":
        suma("airline_ontime_kpi", "on_time_flights") == puntuales_silver,
    "ml_flights solo tiene vuelos con retrasos completos":
        spark.table(f"{GOLD}.ml_flights").count() == silver.filter("delay_data_complete").count(),
}
for regla, ok in reglas.items():
    print("OK  " if ok else "FALLA", regla)
assert all(reglas.values()), "Gold no reconcilia con Silver"

print("Gold_Analytics completada")
