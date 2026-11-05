# Databricks notebook source
# MAGIC %md
# MAGIC # Trabajo final · Apartados 1 a 4
# MAGIC **Arquitecturas ETL y Modelos de IA Aplicados · EAM · 2026-II**
# MAGIC
# MAGIC Somos el equipo de datos de una aerolínea. Procesamos vuelos, aerolíneas y aeropuertos con **Spark y Delta Lake** en Databricks, siguiendo la arquitectura medallón:
# MAGIC
# MAGIC | Apartado | Capa | Resultado en Unity Catalog |
# MAGIC |---|---|---|
# MAGIC | 1 · Ingestión y limpieza básica | Bronze | `aerolinea_tf.bronze.flights`, `airlines`, `airports` |
# MAGIC | 2 · Transformaciones y modelado | Silver | `aerolinea_tf.silver.flights_enriched` y tres tablas de métricas |
# MAGIC | 3 · Análisis avanzado y métricas | Análisis | `aerolinea_tf.analisis.*` y vistas temporales |
# MAGIC | 4 · Capa Gold y negocio | Gold | `aerolinea_tf.gold.*` optimizadas con `OPTIMIZE ... ZORDER BY` |
# MAGIC
# MAGIC **Orden en Unity Catalog:** un catálogo propio para el trabajo (`aerolinea_tf`) y un esquema por etapa: `fuentes` (volumen con los CSV), `bronze`, `silver`, `analisis`, `gold` y `ml` (apartado 6).
# MAGIC
# MAGIC **Cómputo:** Serverless. Cada celda se puede repetir: las escrituras usan `overwrite` y los objetos se crean con `IF NOT EXISTS`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Preparación
# MAGIC ### 0.1 · Catálogo, esquemas y volumen
# MAGIC Si la cuenta no permite `CREATE CATALOG`, crear el catálogo `aerolinea_tf` desde **Catalog → + → Create catalog** y volver a ejecutar la celda.

# COMMAND ----------

import os
import shutil
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOGO = "aerolinea_tf"
LANDING = f"/Volumes/{CATALOGO}/fuentes/landing"
ARCHIVOS = ["flights.csv", "airlines.csv", "airports.csv"]

ESQUEMAS = {
    "fuentes": "Archivos de origen sin modificar (volumen landing)",
    "bronze": "Apartado 1: datos ingeridos, tipados y con ingest_timestamp",
    "silver": "Apartado 2: vuelos enriquecidos y métricas por aerolínea y aeropuerto",
    "analisis": "Apartado 3: análisis temporal y rankings con funciones de ventana",
    "gold": "Apartado 4: KPI de negocio listos para dashboards y base de ML",
    "ml": "Apartado 6: modelo, métricas y predicciones del retraso de llegada",
}

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOGO}")
for esquema, comentario in ESQUEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{esquema} COMMENT '{comentario}'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.fuentes.landing")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.2 · Los tres CSV en el volumen
# MAGIC **Carga manual:** **Catalog → aerolinea_tf → fuentes → landing → Upload to this volume** y subir `flights.csv`, `airlines.csv` y `airports.csv` en la raíz.
# MAGIC
# MAGIC **Carga automática:** si el repositorio está clonado como *Git folder*, la celda copia los archivos que falten desde la carpeta `datos/` de la entrega. Si aun así falta alguno, se detiene con un mensaje.

# COMMAND ----------

presentes = {f.name for f in dbutils.fs.ls(LANDING)}
origen_repo = Path(os.getcwd()).parent / "datos"
for archivo in ARCHIVOS:
    if archivo not in presentes and (origen_repo / archivo).is_file():
        shutil.copyfile(origen_repo / archivo, f"{LANDING}/{archivo}")
        print("Copiado desde el repositorio:", archivo)

presentes = {f.name for f in dbutils.fs.ls(LANDING)}
faltantes = [a for a in ARCHIVOS if a not in presentes]
assert not faltantes, f"Subir al volumen {LANDING} los archivos: {faltantes}"
display(dbutils.fs.ls(LANDING))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apartado 1 · Ingestión y limpieza básica (Bronze)
# MAGIC ### 1.1 · Cargar los DataFrames
# MAGIC Se leen los CSV con un esquema explícito **todo en texto** y `mode="FAILFAST"`: si una fila no tiene la estructura esperada, la carga se detiene en lugar de descartarla en silencio. La conversión de tipos se hace después, de forma controlada.

# COMMAND ----------

def leer_csv(archivo, esquema):
    return (
        spark.read.csv(f"{LANDING}/{archivo}", header=True, schema=esquema,
                       mode="FAILFAST", enforceSchema=False)
        .withColumn("source_file", F.lit(archivo))
    )

flights_raw = leer_csv(
    "flights.csv",
    """flight_id STRING, flight_date STRING, airline_id STRING, origin_airport STRING,
       destination_airport STRING, departure_delay STRING, arrival_delay STRING,
       distance_miles STRING""",
)
airlines_raw = leer_csv("airlines.csv", "airline_id STRING, airline_name STRING")
airports_raw = leer_csv("airports.csv", "airport_id STRING, city STRING, country STRING")

print("Vuelos leídos:", flights_raw.count())
print("Aerolíneas leídas:", airlines_raw.count())
print("Aeropuertos leídos:", airports_raw.count())
display(flights_raw.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 · Convertir tipos
# MAGIC - `flight_date` → `DateType`.
# MAGIC - `departure_delay` y `arrival_delay` vienen como `"25.0"`: se pasan a `double` y luego a `IntegerType` (todos los valores son enteros; se comprueba abajo).
# MAGIC - `distance_miles` → `IntegerType`.
# MAGIC - Los códigos se normalizan con `upper(trim(...))` y los textos vacíos pasan a `NULL`.
# MAGIC
# MAGIC Se usa `try_cast`: un valor que no se pueda convertir queda en `NULL` en vez de detener el proceso (Serverless trabaja con ANSI activado).

# COMMAND ----------

def codigo(c):
    return F.when(F.trim(c) != "", F.upper(F.trim(c)))

def texto(c):
    return F.when(F.trim(c) != "", F.trim(c))

def entero_desde_decimal(c):
    return F.expr(f"try_cast(try_cast({c} AS DOUBLE) AS INT)")

flights_typed = flights_raw.select(
    codigo("flight_id").alias("flight_id"),
    F.expr("try_cast(flight_date AS DATE)").alias("flight_date"),
    codigo("airline_id").alias("airline_id"),
    codigo("origin_airport").alias("origin_airport"),
    codigo("destination_airport").alias("destination_airport"),
    entero_desde_decimal("departure_delay").alias("departure_delay"),
    entero_desde_decimal("arrival_delay").alias("arrival_delay"),
    F.expr("try_cast(distance_miles AS INT)").alias("distance_miles"),
    "source_file",
)
airlines_typed = airlines_raw.select(
    codigo("airline_id").alias("airline_id"), texto("airline_name").alias("airline_name"), "source_file")
airports_typed = airports_raw.select(
    codigo("airport_id").alias("airport_id"), texto("city").alias("city"),
    texto("country").alias("country"), "source_file")

# Ningún retraso tenía decimales: la conversión a entero no pierde información
decimales = flights_raw.filter(
    F.expr("try_cast(departure_delay AS DOUBLE) % 1 != 0 OR try_cast(arrival_delay AS DOUBLE) % 1 != 0")
).count()
print("Retrasos con parte decimal:", decimales)
assert decimales == 0
flights_typed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 · Eliminar filas sin `flight_id`, `airline_id` u `origin_airport`
# MAGIC Antes de filtrar se cuentan los nulos de cada campo obligatorio, para dejar constancia de cuántas filas se descartan y por qué.

# COMMAND ----------

obligatorios = ["flight_id", "airline_id", "origin_airport"]
display(flights_typed.select([F.sum(F.col(c).isNull().cast("int")).alias(f"nulos_{c}") for c in obligatorios]))

flights_clean = flights_typed.dropna(subset=obligatorios)
airlines_clean = airlines_typed.dropna(subset=["airline_id"])
airports_clean = airports_typed.dropna(subset=["airport_id"])

total, limpios = flights_typed.count(), flights_clean.count()
print(f"Vuelos: {total} leídos, {total - limpios} descartados, {limpios} conservados")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 · Columna `ingest_timestamp`

# COMMAND ----------

flights_bronze = flights_clean.withColumn("ingest_timestamp", F.current_timestamp())
airlines_bronze = airlines_clean.withColumn("ingest_timestamp", F.current_timestamp())
airports_bronze = airports_clean.withColumn("ingest_timestamp", F.current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 · Guardar en Delta Lake (Bronze)
# MAGIC `overwrite` reconstruye la instantánea completa en cada ejecución, así que repetir la celda no duplica filas.

# COMMAND ----------

def guardar(df, tabla):
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{CATALOGO}.{tabla}"))
    print(f"{CATALOGO}.{tabla}: {spark.table(f'{CATALOGO}.{tabla}').count()} filas")

guardar(flights_bronze, "bronze.flights")
guardar(airlines_bronze, "bronze.airlines")
guardar(airports_bronze, "bronze.airports")

# COMMAND ----------

conteos_bronze = {t: spark.table(f"{CATALOGO}.bronze.{t}").count() for t in ["flights", "airlines", "airports"]}
print(conteos_bronze)
assert conteos_bronze == {"flights": 49479, "airlines": 6, "airports": 10}, conteos_bronze
display(spark.table(f"{CATALOGO}.bronze.flights").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apartado 2 · Transformaciones y modelado (Silver)
# MAGIC ### 2.1 · Cargar Bronze y revisar esquemas

# COMMAND ----------

flights = spark.table(f"{CATALOGO}.bronze.flights")
airlines = spark.table(f"{CATALOGO}.bronze.airlines")
airports = spark.table(f"{CATALOGO}.bronze.airports")
for nombre, df in [("flights", flights), ("airlines", airlines), ("airports", airports)]:
    print(nombre)
    df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 · Unir aerolíneas y aeropuertos de origen y destino
# MAGIC Se usan `left join` para no perder vuelos aunque falte una referencia. La tabla de aeropuertos se une dos veces con alias distintos (origen y destino). Antes de unir se comprueba que las dimensiones no tengan claves repetidas, para que la unión no multiplique vuelos.

# COMMAND ----------

assert airlines.select("airline_id").distinct().count() == airlines.count()
assert airports.select("airport_id").distinct().count() == airports.count()

origen = airports.select(F.col("airport_id").alias("origin_airport"),
                         F.col("city").alias("origin_city"), F.col("country").alias("origin_country"))
destino = airports.select(F.col("airport_id").alias("destination_airport"),
                          F.col("city").alias("destination_city"), F.col("country").alias("destination_country"))

flights_joined = (
    flights.drop("source_file", "ingest_timestamp")
    .join(airlines.select("airline_id", "airline_name"), "airline_id", "left")
    .join(origen, "origin_airport", "left")
    .join(destino, "destination_airport", "left")
)
assert flights_joined.count() == flights.count(), "La unión multiplicó o perdió vuelos"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 · Nuevas columnas
# MAGIC | Columna | Regla |
# MAGIC |---|---|
# MAGIC | `delay_total` | `departure_delay + arrival_delay` (minutos). Si falta uno de los dos queda `NULL`: no se inventa un retraso. |
# MAGIC | `on_time` | `true` si `arrival_delay <= 15`. Es el criterio habitual de puntualidad (llegar con menos de 15 minutos de retraso). |
# MAGIC | `distance_km` | `distance_miles × 1,609344`, redondeado a 2 decimales. |
# MAGIC | `delay_category` | Según `arrival_delay`: **None** ≤ 0 · **Short** 1–15 · **Medium** 16–45 · **Long** > 45 · **Unknown** si no hay dato. |
# MAGIC
# MAGIC Así `on_time` y `delay_category` son coherentes: un vuelo es puntual exactamente cuando su categoría es *None* o *Short*.

# COMMAND ----------

MILLA_KM = 1.609344

flights_enriched = (
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
)
display(flights_enriched.groupBy("delay_category", "on_time").count().orderBy("delay_category"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 · Agregaciones por aerolínea
# MAGIC Retraso promedio (de llegada, de salida y total), distancia total en km, número de vuelos y porcentaje de vuelos a tiempo. `avg` ignora los `NULL`, así que los porcentajes se calculan solo sobre vuelos con dato de llegada.

# COMMAND ----------

def pct_a_tiempo():
    return F.round(F.avg(F.col("on_time").cast("int")) * 100, 2)

airline_metrics = (
    flights_enriched.groupBy("airline_id", "airline_name")
    .agg(
        F.count("*").alias("num_flights"),
        F.round(F.avg("arrival_delay"), 2).alias("avg_arrival_delay"),
        F.round(F.avg("departure_delay"), 2).alias("avg_departure_delay"),
        F.round(F.avg("delay_total"), 2).alias("avg_delay_total"),
        F.round(F.sum("distance_km"), 2).alias("total_distance_km"),
        pct_a_tiempo().alias("on_time_pct"),
    )
    .orderBy("airline_id")
)
display(airline_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC Porcentaje de vuelos a tiempo **de cada aerolínea por aeropuerto de origen** (6 aerolíneas × 10 aeropuertos).

# COMMAND ----------

airline_origin_ontime = (
    flights_enriched.groupBy("airline_id", "airline_name", "origin_airport", "origin_city")
    .agg(F.count("*").alias("num_flights"), pct_a_tiempo().alias("on_time_pct"))
    .orderBy("airline_id", "origin_airport")
)
display(airline_origin_ontime)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 · Agregaciones por aeropuerto
# MAGIC Cada aeropuerto se mide como **origen** (vuelos salientes: retraso de salida) y como **destino** (vuelos entrantes: retraso de llegada). Las dos vistas se unen con `full outer join` para no perder un aeropuerto que solo aparezca en una de ellas.

# COMMAND ----------

salientes = (
    flights_enriched.groupBy(F.col("origin_airport").alias("airport_id"))
    .agg(F.count("*").alias("outgoing_flights"),
         F.round(F.avg("departure_delay"), 2).alias("avg_delay_as_origin"),
         pct_a_tiempo().alias("on_time_pct_outgoing"))
)
entrantes = (
    flights_enriched.groupBy(F.col("destination_airport").alias("airport_id"))
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
    .orderBy("airport_id")
)
display(airport_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 · Validación y limpieza final
# MAGIC Revisamos los nulos de las columnas importantes:
# MAGIC - **Textos de referencia** (`airline_name`, ciudades y países): si faltaran, se reemplazan por `"Desconocido"` para que los informes agrupen bien.
# MAGIC - **Retrasos**: **no se rellenan**. Un retraso ausente significa que no se conoce, y sustituirlo por 0 (o por la media) inventaría vuelos puntuales y sesgaría los promedios y el modelo del apartado 6. Se conservan como `NULL`, `delay_category = 'Unknown'` y la columna `delay_data_complete` permite filtrarlos.
# MAGIC - **Duplicados**: se comprueba que `flight_id` sea único.

# COMMAND ----------

importantes = ["flight_id", "flight_date", "airline_id", "airline_name", "origin_airport", "origin_city",
               "destination_airport", "destination_city", "departure_delay", "arrival_delay",
               "distance_km", "delay_total", "on_time"]
display(flights_enriched.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in importantes]))

# COMMAND ----------

flights_silver = (
    flights_enriched
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
    F.sum((F.col("origin_airport") == F.col("destination_airport")).cast("int")).alias("origen_igual_destino"),
).first()
print(controles.asDict())
assert controles["vuelos"] == controles["ids_unicos"] == 49479
assert controles["sin_fecha"] == 0 and controles["origen_igual_destino"] == 0
assert controles["aerolinea_desconocida"] == 0 and controles["origen_desconocido"] == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.7 · Guardar en Silver
# MAGIC La tabla de vuelos enriquecidos se particiona por mes para que las consultas por periodo lean menos archivos.

# COMMAND ----------

(flights_silver.withColumn("flight_month", F.date_format("flight_date", "yyyy-MM"))
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .partitionBy("flight_month")
 .saveAsTable(f"{CATALOGO}.silver.flights_enriched"))

guardar(airline_metrics, "silver.airline_metrics")
guardar(airline_origin_ontime, "silver.airline_origin_ontime")
guardar(airport_metrics, "silver.airport_metrics")
print("silver.flights_enriched:", spark.table(f"{CATALOGO}.silver.flights_enriched").count(), "filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apartado 3 · Análisis avanzado y métricas
# MAGIC ### 3.1 · Análisis temporal de retrasos
# MAGIC Se extraen mes, trimestre y día de la semana (lunes = 1). `dayofweek` de Spark empieza en domingo = 1, por eso se convierte a la numeración ISO.

# COMMAND ----------

DIAS = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]

silver = (
    spark.table(f"{CATALOGO}.silver.flights_enriched")
    .withColumn("month", F.month("flight_date"))
    .withColumn("quarter", F.quarter("flight_date"))
    .withColumn("day_of_week", ((F.dayofweek("flight_date") + 5) % 7) + 1)
    .withColumn("day_name", F.element_at(F.array(*[F.lit(d) for d in DIAS]), F.col("day_of_week")))
)

def retraso_por(*columnas):
    return (
        silver.groupBy(*columnas)
        .agg(F.count("*").alias("num_flights"),
             F.round(F.avg("arrival_delay"), 2).alias("avg_arrival_delay"),
             F.round(F.avg("departure_delay"), 2).alias("avg_departure_delay"),
             F.round(F.avg("delay_total"), 2).alias("avg_delay_total"),
             pct_a_tiempo().alias("on_time_pct"))
        .orderBy(*columnas)
    )

delay_by_month = retraso_por("month")
delay_by_weekday = retraso_por("day_of_week", "day_name")
delay_by_quarter = retraso_por("quarter")
display(delay_by_month)

# COMMAND ----------

display(delay_by_weekday)

# COMMAND ----------

display(delay_by_quarter)

# COMMAND ----------

# MAGIC %md
# MAGIC **Lectura:** el retraso de llegada medio es muy estable en el tiempo. Por mes va de 12,97 min (marzo) a 13,74 min (junio). Por día de la semana, el viernes es el mejor (12,66 min y 55,0 % a tiempo) y el miércoles el peor (13,69 min). Entre el primer y el segundo trimestre hay solo 0,25 min de diferencia. El calendario apenas explica los retrasos; como se ve en el apartado 2, la diferencia está en la aerolínea.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 · Top 5 vuelos con mayor retraso total en cada aerolínea: `row_number` frente a `dense_rank`
# MAGIC Ambas funciones ordenan dentro de la ventana `partitionBy("airline_id").orderBy(desc("delay_total"))`:
# MAGIC - **`row_number`** da un número distinto a cada fila (1, 2, 3...), aunque dos vuelos tengan el mismo retraso. Siempre devuelve **exactamente 5 vuelos** por aerolínea; si hay empate en el corte, cuál entra depende del criterio de desempate (aquí `flight_id`).
# MAGIC - **`dense_rank`** da **el mismo puesto a los empates** y no deja huecos (1, 1, 2, 3...). Filtrar `<= 5` devuelve los vuelos de los **5 valores de retraso más altos**, que pueden ser más de 5 vuelos.

# COMMAND ----------

ventana_aerolinea = Window.partitionBy("airline_id").orderBy(F.desc("delay_total"), "flight_id")
ranking_vuelos = (
    silver.filter(F.col("delay_total").isNotNull())
    .withColumn("row_number", F.row_number().over(ventana_aerolinea))
    .withColumn("dense_rank", F.dense_rank().over(
        Window.partitionBy("airline_id").orderBy(F.desc("delay_total"))))
    .select("airline_id", "airline_name", "flight_id", "flight_date", "origin_airport",
            "destination_airport", "departure_delay", "arrival_delay", "delay_total",
            "row_number", "dense_rank")
)

top5_row_number = ranking_vuelos.filter("row_number <= 5")
top5_dense_rank = ranking_vuelos.filter("dense_rank <= 5")
display(top5_dense_rank.orderBy("airline_id", "dense_rank", "row_number"))

# COMMAND ----------

comparacion_rankings = (
    top5_row_number.groupBy("airline_id").agg(F.count("*").alias("vuelos_row_number"))
    .join(top5_dense_rank.groupBy("airline_id").agg(F.count("*").alias("vuelos_dense_rank"),
                                                    F.min("delay_total").alias("retraso_corte_dense_rank")),
          "airline_id")
    .join(top5_row_number.groupBy("airline_id").agg(F.min("delay_total").alias("retraso_corte_row_number")),
          "airline_id")
    .withColumn("vuelos_extra_por_empates", F.col("vuelos_dense_rank") - F.col("vuelos_row_number"))
    .orderBy("airline_id")
)
display(comparacion_rankings)

# COMMAND ----------

# MAGIC %md
# MAGIC **Lectura:** con `row_number` salen 30 vuelos (5 por aerolínea). Con `dense_rank` salen 36, porque en 5 de las 6 aerolíneas hay vuelos con el mismo `delay_total`. En Spirit (NK), por ejemplo, entran 7 vuelos y el corte baja de 174 a 171 minutos. Solo Southwest (WN) no tiene empates y devuelve los mismos 5 vuelos con las dos funciones.
# MAGIC
# MAGIC Para un informe de "los 5 peores vuelos" conviene `row_number`, sabiendo que un empate en el corte se resuelve con un criterio arbitrario (aquí, `flight_id`). Para "todos los vuelos con los 5 peores niveles de retraso", sin dejar fuera ningún empate, conviene `dense_rank`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 · Ranking de aeropuertos según el retraso de sus vuelos salientes
# MAGIC `avg_delay` es el retraso de salida promedio de los vuelos que parten del aeropuerto. `airport_rank = 1` es el aeropuerto con **mayor** retraso promedio.

# COMMAND ----------

airport_delay_rank = (
    silver.groupBy(F.col("origin_airport").alias("airport_id"), F.col("origin_city").alias("city"))
    .agg(F.count("*").alias("outgoing_flights"),
         F.round(F.avg("departure_delay"), 2).alias("avg_delay"),
         pct_a_tiempo().alias("on_time_pct"))
    .withColumn("airport_rank", F.dense_rank().over(Window.orderBy(F.desc("avg_delay"))))
    .orderBy("airport_rank")
)
display(airport_delay_rank)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 · Retraso, % a tiempo y número de vuelos por aerolínea y mes

# COMMAND ----------

airline_month_metrics = (
    silver.groupBy("airline_id", "airline_name", "month")
    .agg(F.count("*").alias("num_flights"),
         F.round(F.avg("arrival_delay"), 2).alias("avg_arrival_delay"),
         F.round(F.avg("delay_total"), 2).alias("avg_delay_total"),
         pct_a_tiempo().alias("on_time_pct"))
    .orderBy("airline_id", "month")
)
display(airline_month_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 · Guardar en Delta Lake y crear vistas temporales
# MAGIC Las tablas quedan en `aerolinea_tf.analisis` para Databricks SQL y dashboards; las vistas temporales sirven para consultar en este mismo notebook.

# COMMAND ----------

resultados_analisis = {
    "delay_by_month": delay_by_month,
    "delay_by_weekday": delay_by_weekday,
    "delay_by_quarter": delay_by_quarter,
    "top5_delayed_flights_row_number": top5_row_number,
    "top5_delayed_flights_dense_rank": top5_dense_rank,
    "ranking_comparison": comparacion_rankings,
    "airport_delay_rank": airport_delay_rank,
    "airline_month_metrics": airline_month_metrics,
}
for nombre, df in resultados_analisis.items():
    guardar(df, f"analisis.{nombre}")
    df.createOrReplaceTempView(f"v_{nombre}")

display(spark.sql("SELECT airline_id, month, num_flights, on_time_pct FROM v_airline_month_metrics WHERE month = 6 ORDER BY on_time_pct DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apartado 4 · Capa Gold y análisis de negocio
# MAGIC ### 4.1 · Tablas agregadas Gold
# MAGIC | Tabla | Pregunta de negocio |
# MAGIC |---|---|
# MAGIC | `gold.delay_airline_airport_month` | ¿Cuánto se retrasa cada aerolínea en cada aeropuerto de origen, mes a mes? |
# MAGIC | `gold.airport_punctuality_ranking` | ¿Qué aeropuertos son los más puntuales? (`punctuality_rank = 1` es el mejor) |
# MAGIC | `gold.airline_ontime_kpi` | KPI de puntualidad por aerolínea, con su posición y la diferencia frente a la media de la red |
# MAGIC | `gold.ml_flights` | Base por vuelo para el modelo del apartado 6 (solo vuelos con retrasos conocidos) |

# COMMAND ----------

delay_airline_airport_month = (
    silver.groupBy("airline_id", "airline_name", F.col("origin_airport").alias("airport_id"),
                   F.col("origin_city").alias("city"), F.col("flight_month").alias("month"))
    .agg(F.count("*").alias("num_flights"),
         F.round(F.avg("departure_delay"), 2).alias("avg_departure_delay"),
         F.round(F.avg("arrival_delay"), 2).alias("avg_arrival_delay"),
         F.round(F.avg("delay_total"), 2).alias("avg_delay_total"),
         pct_a_tiempo().alias("on_time_pct"))
    .orderBy("month", "airline_id", "airport_id")
)

airport_punctuality_ranking = (
    silver.groupBy(F.col("origin_airport").alias("airport_id"), F.col("origin_city").alias("city"),
                   F.col("origin_country").alias("country"))
    .agg(F.count("*").alias("outgoing_flights"),
         F.sum(F.col("on_time").cast("int")).alias("on_time_flights"),
         pct_a_tiempo().alias("on_time_pct"),
         F.round(F.avg("arrival_delay"), 2).alias("avg_arrival_delay"))
    .withColumn("punctuality_rank", F.dense_rank().over(Window.orderBy(F.desc("on_time_pct"))))
    .orderBy("punctuality_rank")
)

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
    .orderBy("ontime_rank")
)

ml_flights = (
    silver.filter("delay_data_complete")
    .select("flight_id", "flight_date", "flight_month", "month", "day_of_week",
            "airline_id", "origin_airport", "destination_airport", "distance_km",
            "departure_delay", "arrival_delay")
)

display(airline_ontime_kpi)

# COMMAND ----------

display(airport_punctuality_ranking)

# COMMAND ----------

guardar(delay_airline_airport_month, "gold.delay_airline_airport_month")
guardar(airport_punctuality_ranking, "gold.airport_punctuality_ranking")
guardar(airline_ontime_kpi, "gold.airline_ontime_kpi")
guardar(ml_flights, "gold.ml_flights")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 · Optimización Delta
# MAGIC - **`OPTIMIZE`** compacta los archivos pequeños que dejan las escrituras en archivos más grandes.
# MAGIC - **`ZORDER BY`** reordena los datos por las columnas de filtro más frecuentes, para que Delta salte archivos completos (*data skipping*) en las consultas de los dashboards: aerolínea y aeropuerto en la tabla mensual y fecha en la base de ML.
# MAGIC - Las tablas de ranking son pequeñas (10 y 6 filas): basta con compactarlas.
# MAGIC
# MAGIC `DESCRIBE HISTORY` muestra la operación `OPTIMIZE` registrada en el log de Delta.

# COMMAND ----------

optimizaciones = {
    "gold.delay_airline_airport_month": "airline_id, airport_id",
    "gold.ml_flights": "flight_date, airline_id",
    "silver.flights_enriched": "airline_id, origin_airport",
    "gold.airport_punctuality_ranking": None,
    "gold.airline_ontime_kpi": None,
}
for tabla, zorder in optimizaciones.items():
    sentencia = f"OPTIMIZE {CATALOGO}.{tabla}" + (f" ZORDER BY ({zorder})" if zorder else "")
    print(sentencia)
    spark.sql(sentencia)

display(spark.sql(f"DESCRIBE HISTORY {CATALOGO}.gold.delay_airline_airport_month")
        .select("version", "timestamp", "operation", "operationParameters"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 · Control final del pipeline
# MAGIC Las tablas Gold deben reconciliar con Silver: los vuelos de cada resumen suman el total de Silver y los vuelos puntuales coinciden.

# COMMAND ----------

total_silver = silver.count()
puntuales_silver = silver.filter("on_time").count()
reglas = {
    "delay_airline_airport_month suma los vuelos de Silver":
        spark.table(f"{CATALOGO}.gold.delay_airline_airport_month").agg(F.sum("num_flights")).first()[0] == total_silver,
    "airport_punctuality_ranking suma los vuelos de Silver":
        spark.table(f"{CATALOGO}.gold.airport_punctuality_ranking").agg(F.sum("outgoing_flights")).first()[0] == total_silver,
    "airline_ontime_kpi suma los vuelos de Silver":
        spark.table(f"{CATALOGO}.gold.airline_ontime_kpi").agg(F.sum("num_flights")).first()[0] == total_silver,
    "airline_ontime_kpi suma los vuelos puntuales de Silver":
        spark.table(f"{CATALOGO}.gold.airline_ontime_kpi").agg(F.sum("on_time_flights")).first()[0] == puntuales_silver,
    "ml_flights solo tiene vuelos con retrasos completos":
        spark.table(f"{CATALOGO}.gold.ml_flights").count() == silver.filter("delay_data_complete").count(),
    "delay_airline_airport_month tiene 6 aerolíneas × 10 aeropuertos × 6 meses":
        spark.table(f"{CATALOGO}.gold.delay_airline_airport_month").count() == 360,
}
for regla, ok in reglas.items():
    print("OK  " if ok else "FALLA", regla)
assert all(reglas.values()), "El control final del pipeline no cuadra"

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOGO}.gold"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusiones de negocio
# MAGIC - **Calidad de datos:** de 50.000 vuelos se descartaron 521 sin `flight_id`. Los 49.479 restantes tienen identificador único. 473 vuelos (0,96 %) no tienen uno de los dos retrasos: se conservan, pero no cuentan en los promedios ni entran en la base de ML.
# MAGIC - **Puntualidad de la red:** el 53,99 % de los vuelos llega con 15 minutos de retraso o menos, y el retraso de llegada medio es de 13,4 minutos.
# MAGIC - **La aerolínea es lo que marca la diferencia.** United, Delta, American y Southwest están entre el 58,8 % y el 60,1 % de puntualidad, unos 5–6 puntos por encima de la red. **JetBlue (42,4 %) y Spirit (42,8 %) quedan unos 11 puntos por debajo**, con el doble de retraso medio (≈ 20 min frente a ≈ 10). La brecha se repite en los 10 aeropuertos y en los 6 meses, así que es un problema de esas aerolíneas, no de un aeropuerto o de una época.
# MAGIC - **Aeropuertos:** el más puntual es Atlanta (54,73 %) y el menos puntual Las Vegas (53,12 %). La diferencia es de solo 1,6 puntos, así que el ranking ordena aeropuertos muy parecidos.
# MAGIC - **Calendario:** meses, días de la semana y trimestres varían menos de un minuto de media.
# MAGIC - **Recomendación:** centrar las acciones de puntualidad en la salida de JetBlue y Spirit. Su retraso de llegada es casi todo retraso de salida, y el apartado 6 muestra que el retraso de salida explica la llegada.
