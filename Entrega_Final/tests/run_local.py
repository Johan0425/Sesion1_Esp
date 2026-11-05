"""Ejecuta la entrega final completa en un Spark local con Delta Lake.

Orden:
    1. notebooks/01_Pipeline_Vuelos_Apartados_1-4      (apartados 1-4)
    2. pipeline/bronze_ingestion.py -> silver_transform.py -> gold_analytics.py
       (las 3 tareas del Job, cada una en un contexto nuevo y con el volumen vacío)
       y comprobación de que Silver y Gold quedan idénticos a los del notebook
    3. notebooks/02_ML_Retraso_Llegada                 (apartado 6)
       y exportación de sus resultados a resultados_ml/

Emula lo mínimo de Databricks:
- /Volumes/... es una carpeta temporal.
- catalogo.esquema.tabla se traduce al esquema local catalogo_esquema.tabla.
- CREATE CATALOG y CREATE VOLUME son exclusivas de Unity Catalog: se registran como OMITIDAS.
- ANSI activado, como en Serverless. MLflow guarda en una carpeta local.

Requisitos: Java 17, pyspark==3.5.3, delta-spark==3.2.1, pandas, scikit-learn,
matplotlib, joblib y mlflow-skinny.
Uso (desde la carpeta de la entrega):
    python tests/run_local.py
"""
import os
import re
import shutil
import sys
import tempfile
import traceback
from pathlib import Path
from types import SimpleNamespace

import matplotlib
matplotlib.use("Agg")

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, DataFrameReader, DataFrameWriter, SparkSession

ENTREGA = Path(__file__).resolve().parent.parent
ENCABEZADO = "# Databricks notebook source"
SEPARADOR = "# COMMAND ----------"
CATALOGO = "aerolinea_tf"
ESQUEMAS = ["fuentes", "bronze", "silver", "analisis", "gold", "ml"]

SOLO_UNITY_CATALOG = re.compile(r"^\s*(CREATE\s+CATALOG|USE\s+CATALOG|CREATE\s+VOLUME)", re.I)
NOMBRE_UC = re.compile(rf"\b({CATALOGO})\.(\w+)")

TABLAS_JOB = [
    "bronze.flights", "bronze.airlines", "bronze.airports",
    "silver.flights_enriched", "silver.airline_metrics", "silver.airline_origin_ontime",
    "silver.airport_metrics", "gold.delay_airline_airport_month",
    "gold.airport_punctuality_ranking", "gold.airline_ontime_kpi", "gold.ml_flights",
]
COLUMNAS_TIEMPO = {"ingest_timestamp", "silver_timestamp"}


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
    create_df = SparkSession.createDataFrame

    def local(nombre):
        return NOMBRE_UC.sub(r"\1_\2", nombre)

    def volumen(ruta):
        return f"{base}{ruta}" if ruta.startswith("/Volumes/") else ruta

    def sql(self, consulta, *a, **k):
        if SOLO_UNITY_CATALOG.match(consulta):
            omitidas.append(consulta.strip().splitlines()[0])
            return create_df(self, [], "omitida STRING")
        return sql_original(self, local(consulta), *a, **k)

    SparkSession.sql = sql
    SparkSession.table = lambda self, n: table_original(self, local(n))
    DataFrameReader.table = lambda self, n: reader_table(self, local(n))
    DataFrameReader.csv = lambda self, ruta, *a, **k: reader_csv(self, volumen(ruta), *a, **k)
    DataFrameWriter.saveAsTable = lambda self, n, *a, **k: save_original(self, local(n), *a, **k)
    return omitidas, sql_original


def preparar_volumen(spark, base, sql_original):
    """Deja el catálogo vacío y el volumen landing creado pero sin archivos."""
    for esquema in ESQUEMAS:
        sql_original(spark, f"DROP DATABASE IF EXISTS {CATALOGO}_{esquema} CASCADE")
        sql_original(spark, f"CREATE DATABASE {CATALOGO}_{esquema}")
    shutil.rmtree(base / "Volumes", ignore_errors=True)
    for volumen in ["fuentes/landing", "ml/modelos"]:
        (base / f"Volumes/{CATALOGO}/{volumen}").mkdir(parents=True)


def dbutils_local(base):
    def ls(ruta):
        carpeta = Path(f"{base}{ruta}" if ruta.startswith("/Volumes/") else ruta)
        return [SimpleNamespace(name=p.name, path=str(p), size=p.stat().st_size) for p in sorted(carpeta.iterdir())]
    return SimpleNamespace(fs=SimpleNamespace(ls=ls))


def display(obj):
    if isinstance(obj, DataFrame):
        obj.show(25, truncate=False)
    elif hasattr(obj, "to_string"):
        print(obj.to_string(max_rows=25))
    elif hasattr(obj, "savefig"):
        print(f"[figura {obj.get_size_inches()}]")
    elif isinstance(obj, list):
        for x in obj:
            print(x)
    else:
        print(obj)


def ejecutar(spark, base, tarea, archivo):
    ruta = ENTREGA / archivo
    es_notebook = ruta.read_text(encoding="utf-8").startswith(ENCABEZADO)
    celdas = celdas_notebook(ruta) if es_notebook else [ruta.read_text(encoding="utf-8")]
    contexto = {"spark": spark, "display": display, "dbutils": dbutils_local(base),
                "__name__": "__main__", "__file__": str(ruta)}
    os.chdir(ruta.parent)  # en Databricks el directorio de trabajo es la carpeta del archivo
    print(f"\n{'=' * 70}\nTAREA {tarea} · {archivo} · {len(celdas)} celda(s)\n{'=' * 70}")
    for i, codigo in enumerate(celdas, 1):
        codigo = codigo.replace('"/Volumes/', f'"{base}/Volumes/')
        try:
            exec(compile(codigo, f"{archivo}[celda {i}]", "exec"), contexto)
        except Exception:
            traceback.print_exc()
            print(f"\nFALLÓ la tarea {tarea} en la celda {i}.")
            sys.exit(1)
    return contexto, len(celdas)


def instantanea(spark):
    datos = {}
    for tabla in TABLAS_JOB:
        df = spark.table(f"{CATALOGO}.{tabla}")
        columnas = sorted(c for c in df.columns if c not in COLUMNAS_TIEMPO)
        datos[tabla] = (columnas, sorted(map(tuple, df.select(columnas).collect()), key=repr))
    return datos


def exportar_resultados_ml(spark, contexto):
    salida = ENTREGA / "resultados_ml"
    salida.mkdir(exist_ok=True)
    contexto["fig"].savefig(salida / "evaluacion_retraso_llegada.png", dpi=130, bbox_inches="tight")
    spark.table(f"{CATALOGO}.ml.metricas_retraso").toPandas().to_csv(salida / "metricas.csv", index=False)
    (spark.table(f"{CATALOGO}.ml.predicciones_retraso").toPandas()
     .sort_values("flight_id").to_csv(salida / "predicciones_evaluacion.csv", index=False))
    print("Resultados de ML exportados a", salida)


def main():
    base = Path(tempfile.mkdtemp(prefix="aerolinea_tf_"))
    os.environ["MLFLOW_TRACKING_URI"] = (base / "mlruns").as_uri()
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("prueba-trabajo-final")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(base / "warehouse"))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={base}")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    omitidas, sql_original = instalar_emulacion(spark, base)
    resultados = []

    # 1. Notebook de los apartados 1 a 4
    preparar_volumen(spark, base, sql_original)
    _, n = ejecutar(spark, base, "notebook_1-4", "notebooks/01_Pipeline_Vuelos_Apartados_1-4.py")
    resultados.append(("notebook_1-4", n))
    del_notebook = instantanea(spark)

    # 2. Job: Bronze -> Silver -> Gold desde cero
    preparar_volumen(spark, base, sql_original)
    for tarea, archivo in [("Bronze_Ingestion", "pipeline/bronze_ingestion.py"),
                           ("Silver_Transform", "pipeline/silver_transform.py"),
                           ("Gold_Analytics", "pipeline/gold_analytics.py")]:
        _, n = ejecutar(spark, base, tarea, archivo)
        resultados.append((tarea, n))
    del_job = instantanea(spark)
    diferentes = [t for t in TABLAS_JOB if del_notebook[t] != del_job[t]]
    assert not diferentes, f"El Job y el notebook no producen las mismas tablas: {diferentes}"
    print(f"\nEl Job y el notebook producen las mismas {len(TABLAS_JOB)} tablas Bronze/Silver/Gold")

    # 3. Machine learning sobre el Gold del Job
    contexto, n = ejecutar(spark, base, "ML", "notebooks/02_ML_Retraso_Llegada.py")
    resultados.append(("ML", n))
    exportar_resultados_ml(spark, contexto)

    print(f"\n{'=' * 70}\nENTREGA COMPLETADA")
    for tarea, n in resultados:
        print(f"  {tarea:<18} OK  ({n} celdas)")
    print(f"Sentencias exclusivas de Unity Catalog omitidas: {len(omitidas)}")
    for s in sorted(set(omitidas)):
        print("  -", s)


if __name__ == "__main__":
    main()
