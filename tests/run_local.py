"""Ejecuta el notebook completo en un Spark local con Delta Lake.

Emula lo mínimo de Databricks para probar la lógica de las demos y de las preguntas:
- El volumen /Volumes/analitica/data/landing es una carpeta temporal (empieza vacía:
  la celda 2.4 debe copiar los CSV desde la carpeta datos del repositorio).
- analitica.data se traduce al esquema local "data".
- Las sentencias exclusivas de Unity Catalog (CREATE CATALOG, VOLUME, GRANT, SHOW GRANTS)
  no existen en Spark local: se registran como OMITIDAS, no se ejecutan.
- La función SQL es_venta_alta se registra como UDF equivalente.

Requisitos: Java 17, pyspark==3.5.3 y delta-spark==3.2.1.
Uso (desde la raíz del repositorio):
    python tests/run_local.py [01_Fundamentos_Databricks.dbc]
"""
import os
import re
import sys
import tempfile
import traceback
from pathlib import Path
from types import SimpleNamespace

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "tools"))
from build_dbc import leer_celdas_dbc  # noqa: E402

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql.types import BooleanType  # noqa: E402

RUTA_VOLUMEN = "/Volumes/analitica/data/landing"
SOLO_UNITY_CATALOG = re.compile(
    r"^\s*(CREATE\s+CATALOG|USE\s+CATALOG|CREATE\s+VOLUME|SHOW\s+VOLUMES|GRANT\s|SHOW\s+GRANTS"
    r"|CREATE\s+OR\s+REPLACE\s+FUNCTION)",
    re.I,
)


def crear_spark(directorio):
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("prueba-notebook")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(directorio / "warehouse"))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={directorio}")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.udf.register("es_venta_alta", lambda x: None if x is None else x > 500, BooleanType())
    return spark


def adaptar(codigo, volumen_local):
    codigo = codigo.replace(RUTA_VOLUMEN, volumen_local)
    codigo = re.sub(r"\bSHOW SCHEMAS IN analitica\b", "SHOW SCHEMAS", codigo)
    return re.sub(r"\banalitica\.data\b", "data", codigo)


def sentencias_sql(celda):
    cuerpo = "\n".join(l for l in celda.splitlines()[1:] if not l.strip().startswith("--"))
    return [s.strip() for s in re.split(r";\s*(?:\n|$)", cuerpo) if s.strip()]


def mostrar(obj, *args, **kwargs):
    if isinstance(obj, DataFrame):
        obj.show(20, truncate=60)
    else:
        for fila in obj:
            print("  ", fila)


def main():
    ruta_dbc = Path(sys.argv[1]) if len(sys.argv) > 1 else RAIZ / "01_Fundamentos_Databricks.dbc"
    celdas = leer_celdas_dbc(ruta_dbc)
    directorio = Path(tempfile.mkdtemp(prefix="prueba_notebook_"))
    volumen = directorio / "Volumes" / "analitica" / "data" / "landing"
    volumen.mkdir(parents=True)

    spark = crear_spark(directorio)
    dbutils = SimpleNamespace(fs=SimpleNamespace(ls=lambda ruta: [
        SimpleNamespace(name=p.name, path=str(p), size=p.stat().st_size)
        for p in sorted(Path(ruta).iterdir())
    ]))
    espacio = {"spark": spark, "dbutils": dbutils, "display": mostrar}
    os.chdir(RAIZ)  # como un Git folder: el directorio actual es el del notebook

    ejecutadas, omitidas, fallos = 0, [], []
    for n, celda in enumerate(celdas, 1):
        if celda.startswith("%md"):
            continue
        titulo = celda.splitlines()[1 if celda.startswith("%sql") else 0][:70]
        print(f"\n===== Celda {n}: {titulo}")
        try:
            if celda.startswith("%sql"):
                for sentencia in sentencias_sql(celda):
                    if SOLO_UNITY_CATALOG.match(sentencia):
                        print("   OMITIDA (solo Unity Catalog):", sentencia.splitlines()[0])
                        omitidas.append((n, sentencia.splitlines()[0]))
                        continue
                    spark.sql(adaptar(sentencia, str(volumen))).show(20, truncate=60)
            else:
                exec(compile(adaptar(celda, str(volumen)), f"celda_{n}", "exec"), espacio)
            ejecutadas += 1
        except Exception:
            traceback.print_exc()
            fallos.append(n)

    print("\n" + "=" * 70)
    print(f"Celdas de código ejecutadas sin error: {ejecutadas}")
    print(f"Sentencias omitidas (solo Unity Catalog): {len(omitidas)}")
    for n, s in omitidas:
        print(f"   celda {n}: {s}")
    print(f"Celdas con error: {fallos or 'ninguna'}")
    spark.stop()
    sys.exit(1 if fallos else 0)


if __name__ == "__main__":
    main()
