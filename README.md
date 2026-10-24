# Sesiones 1 y 2 · Fundamentos de Databricks

**Arquitecturas ETL y Modelos de IA Aplicados · EAM · 2026-II**

Notebook completo de la práctica, de la **Demo 1 a la Demo 4**, con las respuestas a las preguntas de la **diapositiva 57**.

## Contenido

| Archivo | Qué es |
|---|---|
| [`01_Fundamentos_Databricks.py`](01_Fundamentos_Databricks.py) | Notebook en formato fuente de Databricks (se abre directamente desde un Git folder) |
| [`01_Fundamentos_Databricks.dbc`](01_Fundamentos_Databricks.dbc) | El mismo notebook como archivo `.dbc` para **Import** |
| [`RESPUESTAS_PREGUNTAS.md`](RESPUESTAS_PREGUNTAS.md) | Respuestas a las seis preguntas de la diapositiva 57 |
| [`datos/`](datos) | Los cinco CSV de la práctica |
| [`tools/build_dbc.py`](tools/build_dbc.py) | Regenera el `.dbc` a partir del `.py` |
| [`tests/run_local.py`](tests/run_local.py) | Ejecuta el notebook completo en Spark local con Delta Lake |

## Recorrido

| Bloque | Contenido | Diapositiva | Volver a |
|---|---|---|---|
| Demo 1 | Acceso y exploración del entorno | 18 | 19 |
| Demo 2 | Catálogo `analitica`, esquema `data`, volumen `landing`, `tabla_simple`, `inmuebles`, nutrientes | 25 | 26 |
| Demo 3 | Transformaciones, unión, tablas Delta, UPDATE / INSERT / DELETE / MERGE, historial y Time Travel | 44 | 45 |
| Demo 4 | Vista `v_ventas_por_producto`, función `es_venta_alta`, propietario, permisos y linaje | 55 | 56 |
| Preguntas | Seis respuestas con una celda de evidencia cada una | 57 | 58 |

Al final de las demos 2, 3 y 4 hay una celda con `assert` que comprueba los resultados esperados: 3 filas en `tabla_simple`, 8.969 inmuebles, 335 alimentos, 10 pedidos enriquecidos, 100 ventas base, 101 filas en la copia Delta y el precio de `id = 1` pasando de 25,99 a 27,99.

## Cómo usarlo en Databricks

**Opción A · Git folder (recomendada)**
1. **Workspace → Create → Git folder** y pegar la URL de este repositorio.
2. Abrir `01_Fundamentos_Databricks` y conectarlo a **Serverless** (o a un cómputo interactivo compatible con Unity Catalog; un SQL warehouse no ejecuta Python).
3. **Run all**. La celda 2.4 copia los CSV de `datos/` al volumen si todavía no están.

**Opción B · Importar el `.dbc`**
1. **Workspace → Import** y seleccionar `01_Fundamentos_Databricks.dbc`.
2. Subir los cinco CSV de `datos/` con **File → Upload files to volume** a `/Volumes/analitica/data/landing`, en la raíz del volumen.
3. Ejecutar las celdas en orden.

**Permisos necesarios:** `CREATE CATALOG` (o que el administrador cree `analitica`), `CREATE SCHEMA`, `CREATE VOLUME` y `WRITE VOLUME`. Las concesiones a `lectores_clase` (4.4) están comentadas a propósito: hay que quitar los `--` solo cuando el grupo exista y se tenga permiso para concederlas.

## Pruebas

`tests/run_local.py` ejecuta **todas las celdas** del `.dbc` en Spark 3.5.3 + Delta Lake 3.2.1 local. Empieza con el volumen vacío, así que también prueba que la celda 2.4 copie los CSV, y falla si algún `assert` no se cumple.

```bash
pip install pyspark==3.5.3 delta-spark==3.2.1   # requiere Java 17
python tools/build_dbc.py        # regenera el .dbc y verifica que coincide con el .py
python tests/run_local.py        # ejecuta el notebook completo
```

Último resultado: **72 celdas de código sin errores**. Spark local no tiene Unity Catalog, así que el script **omite** 9 sentencias propias de UC: `CREATE CATALOG`, `USE CATALOG`, `CREATE VOLUME`, `SHOW VOLUMES`, `SHOW GRANTS` y la definición SQL de `es_venta_alta`, que se sustituye por una UDF equivalente. Esas sentencias siguen la sintaxis de la guía de la sesión y se ejecutan en Databricks.
