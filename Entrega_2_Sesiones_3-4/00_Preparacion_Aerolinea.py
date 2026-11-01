# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Preparación de la aerolínea
# MAGIC **Arquitecturas ETL y Modelos de IA Aplicados · EAM · 2026-II — Sesiones 3 y 4**
# MAGIC
# MAGIC Primera tarea del Job `Aerolinea_2026II_Pipeline`. Crea el catálogo `aerolinea`, los esquemas `fuentes`, `bronze`, `silver` y `gold`, y el volumen `aerolinea.fuentes.landing`, y comprueba que los cuatro CSV estén en su raíz.
# MAGIC
# MAGIC Todas las celdas usan PySpark (`spark.sql` solo para las sentencias DDL de Unity Catalog, que no tienen equivalente en la API de DataFrames). Se pueden repetir: `IF NOT EXISTS` conserva los objetos ya creados.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 · Comprobación del cómputo
# MAGIC Debe aparecer `id` con valor `0`.

# COMMAND ----------

spark.range(1).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 · Catálogo, esquemas y volumen

# COMMAND ----------

CATALOGO = "aerolinea"
ESQUEMAS = ["fuentes", "bronze", "silver", "gold"]

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOGO}")
spark.sql(f"USE CATALOG {CATALOGO}")

for esquema in ESQUEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{esquema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.fuentes.landing")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOGO}"))
display(spark.sql(f"SHOW VOLUMES IN {CATALOGO}.fuentes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 · Los cuatro CSV en el volumen
# MAGIC **Carga manual (como en clase):** menú **File → Upload files to volume**, seleccionar los cuatro CSV de la carpeta `datos` y elegir como destino `/Volumes/aerolinea/fuentes/landing` (en la raíz, sin subcarpetas).
# MAGIC
# MAGIC **Carga automática:** si este repositorio está clonado como *Git folder*, la celda siguiente copia al volumen los archivos que falten desde la carpeta `datos` de la entrega. En cualquier caso comprueba que estén los cuatro archivos; si falta alguno, la tarea falla y el Job se detiene antes de Bronze.

# COMMAND ----------

import os
import shutil

ruta = "/Volumes/aerolinea/fuentes/landing"
archivos_clase = [
    "aeronaves.csv", "aeropuertos.csv",
    "vuelos_diarios.csv", "mantenimientos_rds.csv",
]

presentes = {f.name for f in dbutils.fs.ls(ruta)}
faltantes = [a for a in archivos_clase if a not in presentes]
origen_repo = os.path.join(os.getcwd(), "datos")

for archivo in faltantes:
    origen = os.path.join(origen_repo, archivo)
    if os.path.isfile(origen):
        shutil.copyfile(origen, ruta + "/" + archivo)
        print("Copiado desde el repositorio:", archivo)

presentes = {f.name for f in dbutils.fs.ls(ruta)}
faltantes = [a for a in archivos_clase if a not in presentes]
assert not faltantes, f"Subir al volumen {ruta} los archivos: {faltantes}"
display(dbutils.fs.ls(ruta))
