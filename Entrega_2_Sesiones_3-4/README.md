# Entrega 2 · Sesiones 3 y 4 · Pipeline de la aerolínea

**Arquitecturas ETL y Modelos de IA Aplicados · EAM · 2026-II**

Pipeline Bronze → Silver → Gold sobre los cuatro CSV de la aerolínea, con **todo el SQL de la guía traducido a PySpark**, un **Gold nuevo** construido sobre las tablas Gold/Silver, los **tres scripts `.py` corregidos y completados**, y el **Job** que ejecuta todo con dependencias en Serverless.

## Contenido

| Archivo | Qué es |
|---|---|
| [`00_Preparacion_Aerolinea.py`](00_Preparacion_Aerolinea.py) | Notebook · catálogo `aerolinea`, esquemas `fuentes/bronze/silver/gold`, volumen `fuentes.landing`; copia y verifica los 4 CSV |
| [`01_Bronze_Aerolinea.py`](01_Bronze_Aerolinea.py) | Notebook · lectura `FAILFAST` con todo como texto + `archivo_origen` y `fecha_ingesta` → `aerolinea.bronze.*` |
| [`02_Silver_Aerolinea.py`](02_Silver_Aerolinea.py) | Notebook · tipificación (`try_cast`), controles de contenido, llaves y referencias con `assert` → `aerolinea.silver.*` |
| [`03_Gold_Aerolinea.py`](03_Gold_Aerolinea.py) | Notebook · las 6 tablas Gold de la guía (enriquecidos, KPI puntualidad, KPI costo, resumen aeronave-mes, base ML) |
| [`04_Gold_Nuevo_Aerolinea.py`](04_Gold_Nuevo_Aerolinea.py) | Notebook · **Gold nuevo** con funciones de ventana + control final del pipeline (reconciliación, plan, `DESCRIBE DETAIL/HISTORY`) |
| [`scripts/01_bronze_ingestion.py`](scripts/01_bronze_ingestion.py) | Script · Bronze tipado en el catálogo `airline_mantenimiento` |
| [`scripts/02_silver_transform.py`](scripts/02_silver_transform.py) | Script · Silver normalizado con validaciones bloqueantes |
| [`scripts/03_gold_analytics.py`](scripts/03_gold_analytics.py) | Script · Gold completo: enriquecidos, KPI y ranking de costo por aeronave (`Window`) |
| [`job/aerolinea_job.json`](job/aerolinea_job.json) | Definición del Job (Jobs API 2.1) con las 8 tareas y sus dependencias |
| [`datos/`](datos) | Los cuatro CSV de la práctica |
| [`tests/run_local.py`](tests/run_local.py) | Ejecuta el Job completo en Spark local + Delta Lake |

## SQL → PySpark

Cada celda SQL de la guía tiene su equivalente con la API de DataFrames:

| SQL de la guía | PySpark en la entrega |
|---|---|
| `CREATE OR REPLACE TEMP VIEW x AS SELECT ...` | DataFrame `x = spark.table(...).select(...)` |
| `TRY_CAST(c AS T)` | `try_cast("c", "T")` → `F.expr("try_cast(...)")` (NULL sin error con ANSI) |
| `UPPER(TRIM(c))`, `SELECT DISTINCT` | `F.upper(F.trim(...))`, `.distinct()` |
| `SUM(CASE WHEN cond THEN 1 ELSE 0 END)` | `contar_si(cond)` → `F.sum(F.when(cond, 1).otherwise(0))` |
| `a / NULLIF(b, 0)` | `dividir(a, b)` → `F.when(b != 0, a / b)` |
| `LEFT ANTI JOIN`, `FULL OUTER JOIN`, `COALESCE` | `.join(..., "left_anti")`, `.join(..., "full_outer")`, `F.coalesce` |
| `date_trunc('MONTH', fecha)` | `F.trunc("fecha", "month")` |
| `UNION ALL` de conteos | `functools.reduce(DataFrame.unionByName, [...])` |
| `CREATE OR REPLACE TABLE t AS SELECT ...` | `df.write.format("delta").mode("overwrite").saveAsTable(t)` |
| `EXPLAIN FORMATTED` | `df.explain("formatted")` |

Solo las sentencias DDL de Unity Catalog (`CREATE CATALOG/SCHEMA/VOLUME`, `SHOW`, `DESCRIBE DETAIL/HISTORY`) se ejecutan con `spark.sql(...)` desde Python, porque no tienen equivalente en la API de DataFrames.

## Gold nuevo (`04_Gold_Nuevo_Aerolinea`)

| Tabla | Filas | Contenido |
|---|---|---|
| `aerolinea.gold.kpi_aeronave` | 10 | Vuelos, puntualidad, cancelación, horas voladas, costo total, costo por vuelo operado y por hora volada, `ranking_costo` y `participacion_costo_pct` |
| `aerolinea.gold.kpi_rutas` | 25 | Volumen y puntualidad por origen → destino, `ranking_en_origen` y participación dentro del origen |
| `aerolinea.gold.kpi_mantenimiento_fabricante` | 9 | Costo por fabricante y tipo, peso de cada tipo dentro del fabricante |
| `aerolinea.gold.costo_acumulado_aeronave` | 100 | Costo acumulado por aeronave en el tiempo y días desde la intervención anterior (`lag`) |

Después reconcilia **11 reglas** (los totales de vuelos, intervenciones y costo de cada resumen deben coincidir con Silver) y comprueba los conteos de referencia de la clase. Si algo no cuadra, la tarea falla y el Job queda en *Failed*.

## Correcciones a los tres scripts `.py`

- `01_bronze_ingestion.py`: nombres de variables `*_PAHT` → `*_PATH`; lee del volumen `/Volumes/aerolinea/fuentes/landing` (una sola carga para todo el Job); verifica conteos al final.
- `02_silver_transform.py`: **faltaba el `)` de cierre** de `mantenimientos_slv` (error de sintaxis); se normalizan llaves (`UPPER(TRIM)`), se añade `ruta_por_revisar`, `costo_usd` pasa a `decimal(14,2)` y las validaciones ahora son `assert` bloqueantes.
- `03_gold_analytics.py`: estaba incompleto y usaba el catálogo `airline_exam` (no existe) → `airline_mantenimiento`; ahora crea la sesión, publica 5 tablas Gold (incluido `ranking_costo_aeronave` con `Window`) y reconcilia costos con Silver.

## Job

El Job `Aerolinea_2026II_Pipeline` (definición en [`job/aerolinea_job.json`](job/aerolinea_job.json)) ejecuta las 8 tareas en Serverless, con *Run if: All succeeded* y como máximo una ejecución a la vez:

| Tarea | Tipo | Ruta | Depende de |
|---|---|---|---|
| `preparacion` | Notebook | `00_Preparacion_Aerolinea` | — |
| `bronze` | Notebook | `01_Bronze_Aerolinea` | preparacion |
| `silver` | Notebook | `02_Silver_Aerolinea` | bronze |
| `gold` | Notebook | `03_Gold_Aerolinea` | silver |
| `gold_nuevo` | Notebook | `04_Gold_Nuevo_Aerolinea` | gold |
| `bronze_py` | Python script | `scripts/01_bronze_ingestion.py` | preparacion |
| `silver_py` | Python script | `scripts/02_silver_transform.py` | bronze_py |
| `gold_py` | Python script | `scripts/03_gold_analytics.py` | silver_py |

La tarea `preparacion` crea el catálogo, los esquemas y el volumen, y copia los 4 CSV de `datos/` si no están en el volumen. Nunca se ejecutan más de 2 tareas en paralelo, dentro del límite de 5 de Free Edition. En el JSON, la ruta del workspace aparece como `<TU_USUARIO>`, que corresponde al usuario de la cuenta donde se crea el Job.

## Pruebas

`tests/run_local.py` ejecuta las 8 tareas en el orden del Job sobre Spark 3.5.3 + Delta Lake 3.2.1 con ANSI activado (como Serverless), cada tarea en un contexto nuevo y con el volumen vacío al inicio.

```bash
pip install pyspark==3.5.3 delta-spark==3.2.1   # requiere Java 17
python tests/run_local.py
```

Último resultado: **las 8 tareas OK**, con todos los `assert` cumplidos — 100/10/5/100 registros, 0 errores de calidad, 25 rutas por revisar, 69 operados, 57,97 % de puntualidad, 31 % de cancelación y 2.589.355,38 USD reconciliados en las 11 reglas. Se omiten localmente 6 sentencias exclusivas de Unity Catalog (`CREATE CATALOG`, `USE CATALOG`, `CREATE VOLUME`, `SHOW SCHEMAS`, `SHOW VOLUMES`).
