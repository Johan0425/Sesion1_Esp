"""Ejecuta el Job completo de la entrega en un Spark local con Delta Lake.

Sigue el mismo orden y las mismas dependencias que job/aerolinea_job.json:
    preparacion -> bronze -> silver -> gold -> gold_nuevo
    preparacion -> bronze_py -> silver_py -> gold_py

Emula lo mínimo de Databricks:
- /Volumes/... es una carpeta temporal que empieza vacía (la tarea preparacion debe copiar los CSV).
- Los nombres de tres niveles catalogo.esquema.tabla se traducen a esquema local catalogo_esquema.tabla.
- CREATE CATALOG, USE CATALOG, CREATE VOLUME y SHOW VOLUMES son exclusivas de Unity Catalog:
  se registran como OMITIDAS.
- ANSI activado, como en Serverless.
Cada tarea se ejecuta en un contexto nuevo (no comparte variables con la anterior), como en un Job.

Requisitos: Java 17, pyspark==3.5.3 y delta-spark==3.2.1.
Uso (desde la carpeta de la entrega):
    python tests/run_local.py
"""
import os
import re
import sys
import tempfile
import traceback
from pathlib import Path
from types import SimpleNamespace

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, DataFrameReader, DataFrameWriter, SparkSession

ENTREGA = Path(__file__).resolve().parent.parent
ENCABEZADO = "# Databricks notebook source"
SEPARADOR = "# COMMAND ----------"

TAREAS = [
    ("preparacion", "00_Preparacion_Aerolinea.py"),
    ("bronze", "01_Bronze_Aerolinea.py"),
    ("silver", "02_Silver_Aerolinea.py"),
    ("gold", "03_Gold_Aerolinea.py"),
    ("gold_nuevo", "04_Gold_Nuevo_Aerolinea.py"),
    ("bronze_py", "scripts/01_bronze_ingestion.py"),
    ("silver_py", "scripts/02_silver_transform.py"),
    ("gold_py", "scripts/03_gold_analytics.py"),
]

SOLO_UNITY_CATALOG = re.compile(r"^\s*(CREATE\s+CATALOG|USE\s+CATALOG|CREATE\s+VOLUME|SHOW\s+VOLUMES|SHOW\s+SCHEMAS)", re.I)
NOMBRE_UC = re.compile(r"\b(aerolinea|airline_mantenimiento)\.(\w+)")


def celdas_notebook(ruta):
    texto = ruta.read_text(encoding="utf-8")
    assert texto.startswith(ENCABEZADO), f"{ruta} no es un notebook fuente de Databricks"
    celdas = []
    for bloque in texto[len(ENCABEZADO):].split(SEPARADOR):
        lineas = bloque.strip("\n").splitlines()
        if not lineas or all(l.startswith("# MAGIC") for l in lineas):
            continue  # celda vacía o Markdown
        celdas.append("\n".join(lineas))
    return celdas


def instalar_emulacion(spark, base):
    omitidas = []
    sql_original = SparkSession.sql
    table_original = SparkSession.table
    reader_table = DataFrameReader.table
    reader_csv = DataFrameReader.csv
    save_original = DataFrameWriter.saveAsTable

    def local(nombre):
        return NOMBRE_UC.sub(r"\1_\2", nombre)

    def volumen(ruta):
        return f"{base}{ruta}" if ruta.startswith("/Volumes/") else ruta

    def sql(self, consulta, *a, **k):
        if SOLO_UNITY_CATALOG.match(consulta):
            omitidas.append(consulta.strip().splitlines()[0])
            return self.createDataFrame([], "omitida STRING")
        return sql_original(self, local(consulta), *a, **k)

    SparkSession.sql = sql
    SparkSession.table = lambda self, n: table_original(self, local(n))
    DataFrameReader.table = lambda self, n: reader_table(self, local(n))
    DataFrameReader.csv = lambda self, ruta, *a, **k: reader_csv(self, volumen(ruta), *a, **k)
    DataFrameWriter.saveAsTable = lambda self, n, *a, **k: save_original(self, local(n), *a, **k)

    # Esquemas que crearía Unity Catalog bajo cada catálogo
    for esquema in ["fuentes", "bronze", "silver", "gold"]:
        sql_original(spark, f"CREATE DATABASE IF NOT EXISTS aerolinea_{esquema}")
    (base / "Volumes/aerolinea/fuentes/landing").mkdir(parents=True)
    return omitidas


def dbutils_local(base):
    def ls(ruta):
        carpeta = Path(f"{base}{ruta}" if ruta.startswith("/Volumes/") else ruta)
        return [SimpleNamespace(name=p.name, path=str(p), size=p.stat().st_size) for p in sorted(carpeta.iterdir())]
    return SimpleNamespace(fs=SimpleNamespace(ls=ls))


def display(obj):
    if isinstance(obj, DataFrame):
        obj.show(25, truncate=False)
    elif isinstance(obj, list):
        for x in obj:
            print(x)
    else:
        print(obj)


def main():
    base = Path(tempfile.mkdtemp(prefix="aerolinea_"))
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("prueba-job-aerolinea")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(base / "warehouse"))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={base}")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    omitidas = instalar_emulacion(spark, base)
    os.chdir(ENTREGA)  # en Databricks el directorio de trabajo es la carpeta del notebook

    resultados = []
    for tarea, archivo in TAREAS:
        ruta = ENTREGA / archivo
        es_notebook = ruta.read_text(encoding="utf-8").startswith(ENCABEZADO)
        celdas = celdas_notebook(ruta) if es_notebook else [ruta.read_text(encoding="utf-8")]
        contexto = {"spark": spark, "display": display, "dbutils": dbutils_local(base), "__name__": "__main__"}
        print(f"\n{'=' * 70}\nTAREA {tarea} · {archivo} · {len(celdas)} celda(s)\n{'=' * 70}")
        for i, codigo in enumerate(celdas, 1):
            codigo = codigo.replace('"/Volumes/', f'"{base}/Volumes/')
            try:
                exec(compile(codigo, f"{archivo}[celda {i}]", "exec"), contexto)
            except Exception:
                traceback.print_exc()
                print(f"\nFALLÓ la tarea {tarea} en la celda {i}. Las tareas siguientes no se ejecutan.")
                sys.exit(1)
        resultados.append((tarea, len(celdas)))

    print(f"\n{'=' * 70}\nJOB COMPLETADO")
    for tarea, n in resultados:
        print(f"  {tarea:<12} OK  ({n} celdas)")
    print(f"Sentencias exclusivas de Unity Catalog omitidas: {len(omitidas)}")
    for s in omitidas:
        print("  -", s)


if __name__ == "__main__":
    main()
