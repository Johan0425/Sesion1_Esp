# Arquitecturas ETL y Modelos de IA Aplicados

**EAM · Especialización en Analítica de Datos Aplicada a los Negocios · 2026-II**

Repositorio con las entregas del módulo, desarrolladas y ejecutadas en Databricks (Spark, Delta Lake y Unity Catalog).

| Entrega | Carpeta | Contenido |
|---|---|---|
| **Trabajo final** (sesión 5) | [`Entrega_Final/`](Entrega_Final) | Apartados 1 a 6: pipeline Bronze → Silver → Gold de 50.000 vuelos, análisis con funciones de ventana, KPI de negocio, Job de 3 tareas y modelo de regresión del retraso de llegada con MLflow |
| Entrega 2 (sesiones 3 y 4) | [`Entrega_2_Sesiones_3-4/`](Entrega_2_Sesiones_3-4) | Pipeline Bronze → Silver → Gold de la aerolínea en PySpark, Gold nuevo con funciones de ventana, scripts corregidos y Job de 8 tareas |
| Entrega 1 (sesiones 1 y 2) | esta carpeta (raíz) | Notebook de Fundamentos de Databricks, demos 1 a 4, y respuestas a las preguntas de la diapositiva 57 |

Cada carpeta tiene su propio README con el detalle de la entrega.

---

## Entrega 1 · Sesiones 1 y 2 · Fundamentos de Databricks

Notebook completo de la práctica, de la **Demo 1 a la Demo 4**, con las respuestas a las preguntas de la **diapositiva 57**.

### Contenido

| Archivo | Qué es |
|---|---|
| [`01_Fundamentos_Databricks.py`](01_Fundamentos_Databricks.py) | Notebook en formato fuente de Databricks |
| [`01_Fundamentos_Databricks.dbc`](01_Fundamentos_Databricks.dbc) | El mismo notebook en formato `.dbc` |
| [`RESPUESTAS_PREGUNTAS.md`](RESPUESTAS_PREGUNTAS.md) | Respuestas a las seis preguntas de la diapositiva 57 |
| [`datos/`](datos) | Los cinco CSV de la práctica |
| [`tools/build_dbc.py`](tools/build_dbc.py) | Genera el `.dbc` a partir del `.py` |
| [`tests/run_local.py`](tests/run_local.py) | Ejecuta el notebook completo en Spark local con Delta Lake |

### Recorrido

| Bloque | Contenido | Diapositiva |
|---|---|---|
| Demo 1 | Acceso y exploración del entorno | 18 |
| Demo 2 | Catálogo `analitica`, esquema `data`, volumen `landing`, `tabla_simple`, `inmuebles`, nutrientes | 25 |
| Demo 3 | Transformaciones, unión, tablas Delta, UPDATE / INSERT / DELETE / MERGE, historial y Time Travel | 44 |
| Demo 4 | Vista `v_ventas_por_producto`, función `es_venta_alta`, propietario, permisos y linaje | 55 |
| Preguntas | Seis respuestas con una celda de evidencia cada una | 57 |

Al final de las demos 2, 3 y 4 hay una celda con `assert` que comprueba los resultados esperados: 3 filas en `tabla_simple`, 8.969 inmuebles, 335 alimentos, 10 pedidos enriquecidos, 100 ventas base, 101 filas en la copia Delta y el precio de `id = 1` pasando de 25,99 a 27,99.

Las concesiones de permisos al grupo `lectores_clase` (celda 4.4) quedan comentadas, porque requieren que el grupo exista en el workspace.

### Ejecución

El notebook se ejecuta en Serverless desde un Git folder. La celda 2.4 copia los CSV de `datos/` al volumen `/Volumes/analitica/data/landing`. Requiere permisos `CREATE CATALOG`, `CREATE SCHEMA`, `CREATE VOLUME` y `WRITE VOLUME`.

`tests/run_local.py` ejecuta **todas las celdas** en Spark 3.5.3 + Delta Lake 3.2.1 partiendo del volumen vacío: **72 celdas de código sin errores** y todos los `assert` cumplidos. Spark local no tiene Unity Catalog, por lo que se omiten 9 sentencias propias de UC (`CREATE CATALOG`, `USE CATALOG`, `CREATE VOLUME`, `SHOW VOLUMES`, `SHOW GRANTS` y la definición SQL de `es_venta_alta`, sustituida localmente por una UDF equivalente).
