# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Fundamentos de Databricks
# MAGIC **Arquitecturas ETL y Modelos de IA Aplicados · EAM · 2026-II — Sesiones 1 y 2**
# MAGIC
# MAGIC Notebook de la práctica, desde la Demo 1 hasta la Demo 4, más las respuestas a las preguntas de la diapositiva 57.
# MAGIC
# MAGIC | Bloque | Contenido | Diapositiva de inicio | Volver a |
# MAGIC |---|---|---|---|
# MAGIC | Demo 1 | Acceso y exploración del entorno | 18 | 19 · Jerarquía de los objetos de datos |
# MAGIC | Demo 2 | Objetos de Unity Catalog y primeros DataFrames | 25 | 26 · Spark y Delta Lake |
# MAGIC | Demo 3 | Exploración con Spark y operaciones Delta | 44 | 45 · Gobernanza de datos |
# MAGIC | Demo 4 | Publicación y control de acceso | 55 | 56 · Comprobaciones antes de continuar |
# MAGIC | Preguntas | Preguntas para revisar lo aprendido | 57 | 58 · Continuidad con las sesiones 3 y 4 |
# MAGIC
# MAGIC **Objetos de clase:** catálogo `analitica`, esquema `data`, volumen `landing` → ruta `/Volumes/analitica/data/landing`.
# MAGIC
# MAGIC **Cómputo:** Serverless para notebooks o un cómputo interactivo compatible con Unity Catalog (un SQL warehouse no ejecuta las celdas Python).
# MAGIC
# MAGIC Ejecutar las celdas en orden. Todas las celdas se pueden repetir: las tablas de práctica usan `CREATE OR REPLACE` u `overwrite`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo 1 · Acceso y exploración del entorno
# MAGIC Identificamos la cuenta, el notebook y su cómputo. Si aparece una solicitud de conexión, seleccionamos **Serverless** (o el recurso de clase).
# MAGIC
# MAGIC ### 1.4 · Primera celda
# MAGIC Esperamos una tabla con la columna `id` y el valor `0`.

# COMMAND ----------

spark.range(1).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 · Identidad y ubicación actual
# MAGIC Quién utiliza el notebook y dónde busca los objetos cuando no escribimos el catálogo y el esquema.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user() AS usuario,
# MAGIC        current_catalog() AS catalogo,
# MAGIC        current_schema() AS esquema

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 · Catálogos disponibles
# MAGIC `analitica` puede no aparecer todavía: lo creamos en la demo 2. `samples` es de solo lectura y no será destino de escrituras.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %md
# MAGIC **Comprobación de la demo 1:** identificamos nuestra cuenta, el notebook y su cómputo.
# MAGIC
# MAGIC ➡️ **Fin de la demo 1 · Volver a la diapositiva 19: Jerarquía de los objetos de datos.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo 2 · Objetos de Unity Catalog y primeros DataFrames
# MAGIC ### 2.1 · Creación y selección del catálogo `analitica`
# MAGIC Crear un catálogo requiere `CREATE CATALOG` en el metastore y almacenamiento administrado. Si falta el permiso, el administrador debe preparar `analitica`; luego continuamos con `USE CATALOG`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS analitica

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG analitica

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 · Creación del esquema `data`
# MAGIC `analitica.data` indica que el esquema `data` pertenece al catálogo `analitica`. La segunda instrucción lo deja activo.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS analitica.data;
# MAGIC USE SCHEMA analitica.data;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN analitica

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 · Creación del volumen `landing`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS analitica.data.landing

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN analitica.data

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 · Carga de los CSV al volumen
# MAGIC **Carga manual (como en clase):** menú **File → Upload files to volume**, seleccionar los cinco CSV de la carpeta `datos` y elegir como destino `/Volumes/analitica/data/landing` (en la raíz, sin subcarpetas).
# MAGIC
# MAGIC **Carga automática (opcional):** si este repositorio está clonado como *Git folder* en Databricks, la celda siguiente copia al volumen los archivos que falten desde la carpeta `datos` del repositorio. En cualquier caso, la celda comprueba que estén los cinco archivos.

# COMMAND ----------

import os
import shutil

ruta = "/Volumes/analitica/data/landing"
archivos_clase = [
    "data_demo.csv", "nutrients_csvfile.csv", "ventas_2025.csv",
    "spark_intro.csv", "dim_spark_intro.csv",
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 · Primera tabla creada con SQL
# MAGIC `USING DELTA` hace explícito el formato. `CREATE OR REPLACE` reinicia únicamente esta tabla de práctica.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tabla_simple (
# MAGIC   id INT,
# MAGIC   nombre STRING
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO tabla_simple VALUES (1, 'Ana'), (2, 'Luis'), (3, 'María')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tabla_simple ORDER BY id

# COMMAND ----------

# MAGIC %md
# MAGIC Esperamos tres filas. Si repetimos el `INSERT` sin recrear la tabla, se añaden otras tres: para reiniciar, ejecutar primero `CREATE OR REPLACE` y después un único `INSERT`.
# MAGIC
# MAGIC ### 2.6 · Ruta y funciones de PySpark

# COMMAND ----------

from pyspark.sql import functions as F

ruta = "/Volumes/analitica/data/landing"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.7 · Lectura de inmuebles
# MAGIC `header` usa la primera línea como nombres de columna e `inferSchema` estima los tipos a partir del contenido.

# COMMAND ----------

df_inmuebles = spark.read.csv(
    ruta + "/data_demo.csv", header=True, inferSchema=True
)

display(df_inmuebles.limit(10))

# COMMAND ----------

df_inmuebles.printSchema()
print("Inmuebles:", df_inmuebles.count())

# COMMAND ----------

# MAGIC %md
# MAGIC El archivo contiene 8.969 registros. El CSV no declara la moneda de `value` ni la unidad de `useful_area`, así que no asignamos unidades ni hacemos conversiones.
# MAGIC
# MAGIC ### 2.8 · Guardar el DataFrame como tabla
# MAGIC La lectura crea un DataFrame. Ahora lo guardamos como tabla Delta administrada y la consultamos por su nombre.

# COMMAND ----------

df_inmuebles.write.format("delta").mode("overwrite").saveAsTable("inmuebles")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT rooms, garages, useful_area, value FROM inmuebles LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.9 · Nutrientes y tipos de datos
# MAGIC Leemos `nutrients_csvfile.csv` como texto para conservar los valores originales: algunos campos que parecen numéricos contienen letras (`t`), separadores (`1,373`) o valores ausentes.

# COMMAND ----------

df_nutrientes = spark.read.csv(
    ruta + "/nutrients_csvfile.csv", header=True, inferSchema=False
)

display(df_nutrientes.select("Food", "Calories", "Fat", "Category").limit(10))

# COMMAND ----------

df_nutrientes.printSchema()
print("Alimentos:", df_nutrientes.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN analitica.data

# COMMAND ----------

# MAGIC %md
# MAGIC **Comprobación de la demo 2:** cinco archivos en `landing`, `tabla_simple` con 3 filas, `inmuebles` con 8.969 y el DataFrame de nutrientes con 335 filas de texto.

# COMMAND ----------

assert spark.table("tabla_simple").count() == 3, "Recrear tabla_simple y ejecutar un único INSERT."
assert spark.table("inmuebles").count() == 8969, "Revisar la lectura de data_demo.csv."
assert df_nutrientes.count() == 335, "Revisar la lectura de nutrients_csvfile.csv."
assert all(t == "string" for _, t in df_nutrientes.dtypes), "Nutrientes debe leerse como texto."
print("Demo 2 comprobada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ➡️ **Fin de la demo 2 · Volver a la diapositiva 26: Spark y Delta Lake.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo 3 · Exploración con Spark y operaciones Delta
# MAGIC Primero usamos diez pedidos para ver las transformaciones; después, las operaciones de tabla con `ventas_2025.csv`.
# MAGIC
# MAGIC ### 3.1 · Lectura de pedidos
# MAGIC Diez pedidos de enero de 2024, sin moneda declarada. Usamos tipos explícitos.

# COMMAND ----------

df_pedidos = spark.read.csv(
    ruta + "/spark_intro.csv", header=True,
    schema="""order_id INT, order_date DATE, country STRING, product STRING,
              category STRING, units_sold INT, unit_price DOUBLE""",
    dateFormat="yyyy-MM-dd", mode="FAILFAST"
)

display(df_pedidos)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 · Selección y filtro
# MAGIC El filtro devuelve las filas que cumplen la condición y deja intacto el DataFrame original.

# COMMAND ----------

display(
    df_pedidos.select("order_id", "country", "product", "units_sold")
    .filter(F.col("country") == "Spain")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 · Columna calculada
# MAGIC Interpretamos `unit_price` como precio por unidad: `importe = units_sold * unit_price`.

# COMMAND ----------

df_importes = df_pedidos.withColumn(
    "importe", F.col("units_sold") * F.col("unit_price")
)

display(df_importes)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 · Agrupación
# MAGIC Cada fila del resultado corresponde a un país.

# COMMAND ----------

resumen_pais = df_importes.groupBy("country").agg(
    F.sum("units_sold").alias("unidades"),
    F.round(F.sum("importe"), 2).alias("importe_total")
)

display(resumen_pais.orderBy(F.desc("importe_total")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 · Lectura de la dimensión de productos
# MAGIC Cuatro productos con su familia y proveedor. La llave de la dimensión es `product`.

# COMMAND ----------

df_productos = spark.read.csv(
    ruta + "/dim_spark_intro.csv", header=True,
    schema="product STRING, product_family STRING, supplier STRING",
    mode="FAILFAST"
)

display(df_productos)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.6 · Unión sencilla y comprobación
# MAGIC Primero revisamos que la dimensión tenga una fila por producto (no esperamos filas). Después hacemos una unión izquierda: esperamos conservar los diez pedidos y no encontrar productos sin correspondencia.

# COMMAND ----------

display(df_productos.groupBy("product").count().filter("count > 1"))

df_enriquecido = df_importes.join(df_productos, "product", "left")

display(df_enriquecido)
print("Pedidos antes:", df_pedidos.count())
print("Pedidos después:", df_enriquecido.count())
display(df_pedidos.join(df_productos, "product", "left_anti"))

assert df_enriquecido.count() == df_pedidos.count(), "Revisar la llave de productos."

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.7 · Guardar y volver a consultar
# MAGIC La tabla persistente puede consultarse por nombre desde otro notebook con los permisos necesarios.

# COMMAND ----------

df_enriquecido.write.format("delta").mode("overwrite").saveAsTable("pedidos_enriquecidos")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product, product_family, supplier, importe FROM pedidos_enriquecidos ORDER BY order_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.8 · Lectura del ejemplo de ventas
# MAGIC Cien filas. `cantidad` representa unidades y `precio` el precio unitario; no hay moneda declarada. Cada `id` identifica un registro de venta.

# COMMAND ----------

df_ventas = spark.read.csv(
    ruta + "/ventas_2025.csv", header=True,
    schema="id INT, fecha DATE, producto STRING, cantidad INT, precio DOUBLE",
    dateFormat="yyyy-MM-dd", mode="FAILFAST"
)

display(df_ventas.limit(10))
df_ventas.printSchema()
print("Registros de venta:", df_ventas.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.9 · Revisión básica antes de guardar
# MAGIC Llaves repetidas, nulos y valores no válidos. No esperamos filas en estas dos salidas.

# COMMAND ----------

display(df_ventas.groupBy("id").count().filter("count > 1"))

df_invalidos = df_ventas.filter(
    F.col("id").isNull() | F.col("fecha").isNull() |
    F.col("producto").isNull() | F.col("cantidad").isNull() |
    F.col("precio").isNull() | F.isnan("precio") |
    (F.col("cantidad") <= 0) | (F.col("precio") < 0)
)
display(df_invalidos)
assert df_invalidos.count() == 0, "Revisar los registros antes de continuar."

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.10 · Tabla Delta de ventas
# MAGIC Tabla base con los cien registros del CSV. Los cambios se harán en una copia de ensayo.

# COMMAND ----------

df_ventas.write.format("delta").mode("overwrite").saveAsTable("ventas")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL ventas

# COMMAND ----------

# MAGIC %md
# MAGIC En el resultado de `DESCRIBE DETAIL` comprobamos que `format` es `delta`.
# MAGIC
# MAGIC ### 3.11 · Importe por producto
# MAGIC ### 3.12 · Visualización
# MAGIC Sobre la salida de esta celda: **+ → Visualization → Bar**, eje X `producto`, eje Y `importe_total`, **Save**, y nombre de pestaña *Importe por producto · muestra de ventas 2025*. No añadimos símbolo de moneda porque el archivo no la declara.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT producto,
# MAGIC        SUM(cantidad) AS unidades,
# MAGIC        ROUND(SUM(cantidad * precio), 2) AS importe_total
# MAGIC FROM ventas
# MAGIC GROUP BY producto
# MAGIC ORDER BY importe_total DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.13 · Una variable para cambiar el filtro
# MAGIC Cambiar `pais_elegido` por `Spain`, `France` o `Germany` y volver a ejecutar. El filtro no modifica la tabla guardada.

# COMMAND ----------

pais_elegido = "Spain"

display(df_enriquecido.filter(F.col("country") == pais_elegido))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.14 · Copia de ensayo para Delta
# MAGIC `ventas_delta_demo` es independiente de `ventas`. `CREATE OR REPLACE` reinicia sus datos antes de repetir la secuencia.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ventas_delta_demo USING DELTA AS SELECT * FROM ventas

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.15 · Versión de referencia
# MAGIC Guardamos la versión actual antes de modificar filas. Al repetir el ejercicio puede tener otro número.

# COMMAND ----------

historial = spark.sql("DESCRIBE HISTORY ventas_delta_demo")
version_inicial = historial.orderBy(F.desc("version")).first()["version"]

print("Versión de referencia:", version_inicial)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.16 · UPDATE
# MAGIC El precio de `id = 1` pasa de 25,99 a 26,99 en la copia de ensayo.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE ventas_delta_demo SET precio = ROUND(precio + 1, 2) WHERE id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ventas_delta_demo WHERE id = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.17 · INSERT
# MAGIC Venta ficticia `id = 1001`: la tabla pasa de 100 a 101 filas. Ejecutar la inserción una sola vez dentro de la secuencia.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ventas_delta_demo VALUES (1001, DATE '2025-01-31', 'Producto demo', 2, 10.0)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS registros FROM ventas_delta_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.18 · DELETE
# MAGIC Eliminamos únicamente la venta ficticia: la tabla vuelve a 100 filas.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM ventas_delta_demo WHERE id = 1001

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS registros FROM ventas_delta_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.19 · Fuente de cambios para MERGE
# MAGIC `id = 1` ya existe y llega con precio 27,99; `id = 1001` es nuevo. La fuente usa los mismos nombres de columna que el destino.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW cambios_ventas AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, DATE '2025-01-03', 'Teclado', 20, CAST(27.99 AS DOUBLE)),
# MAGIC   (1001, DATE '2025-01-31', 'Producto demo', 2, CAST(10.0 AS DOUBLE))
# MAGIC AS cambios(id, fecha, producto, cantidad, precio)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cambios_ventas

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.20 · MERGE
# MAGIC Si la llave existe, actualizamos; si no existe, insertamos. La fuente debe tener una sola fila por llave.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ventas_delta_demo AS destino
# MAGIC USING cambios_ventas AS fuente
# MAGIC ON destino.id = fuente.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ventas_delta_demo WHERE id IN (1, 1001) ORDER BY id

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.21 · Historial

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY ventas_delta_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.22 · Time Travel
# MAGIC La versión de referencia tiene 100 filas y precio 25,99 para `id = 1`; la actual tiene 101 filas y precio 27,99.

# COMMAND ----------

df_inicial = spark.read.option("versionAsOf", version_inicial).table("ventas_delta_demo")

print("Filas iniciales:", df_inicial.count())
print("Filas actuales:", spark.table("ventas_delta_demo").count())
display(df_inicial.filter(F.col("id") == 1))
display(spark.table("ventas_delta_demo").filter(F.col("id") == 1))

# COMMAND ----------

# MAGIC %md
# MAGIC **Comprobación de la demo 3:** diez pedidos enriquecidos, cien ventas base y ciento una filas en la copia Delta.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Pedidos enriquecidos' AS tabla, COUNT(*) AS registros FROM pedidos_enriquecidos
# MAGIC UNION ALL
# MAGIC SELECT 'Ventas base', COUNT(*) FROM ventas
# MAGIC UNION ALL
# MAGIC SELECT 'Ensayo Delta', COUNT(*) FROM ventas_delta_demo

# COMMAND ----------

precio_actual = spark.table("ventas_delta_demo").filter("id = 1").first()["precio"]
precio_inicial = df_inicial.filter("id = 1").first()["precio"]

assert spark.table("pedidos_enriquecidos").count() == 10
assert spark.table("ventas").count() == 100
assert spark.table("ventas_delta_demo").count() == 101
assert df_inicial.count() == 100
assert (precio_inicial, precio_actual) == (25.99, 27.99), (precio_inicial, precio_actual)
print("Demo 3 comprobada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ➡️ **Fin de la demo 3 · Volver a la diapositiva 45: Gobernanza de datos.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo 4 · Vistas, funciones y control de acceso
# MAGIC Trabajamos sobre `ventas`, que conserva el CSV original. La copia de ensayo permanece separada.
# MAGIC
# MAGIC ### 4.1 · Creación de una vista

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW v_ventas_por_producto AS
# MAGIC SELECT producto,
# MAGIC        SUM(cantidad) AS unidades,
# MAGIC        ROUND(SUM(cantidad * precio), 2) AS importe_total
# MAGIC FROM ventas
# MAGIC GROUP BY producto

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_ventas_por_producto ORDER BY importe_total DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 · Función reutilizable
# MAGIC Regla de demostración: un importe supera el umbral si es mayor que 500 (no es una clasificación oficial del negocio). Un valor ausente produce `NULL`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION es_venta_alta(importe DOUBLE)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN importe > 500

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, producto, cantidad, precio,
# MAGIC        es_venta_alta(cantidad * precio) AS supera_umbral
# MAGIC FROM ventas
# MAGIC ORDER BY id
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT es_venta_alta(NULL) AS con_valor_ausente

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 · Propiedad y permisos
# MAGIC Propietario de la vista (fila `Owner`) y concesiones que nuestro usuario puede ver.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED v_ventas_por_producto

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON VIEW v_ventas_por_producto

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 · Concesión de acceso
# MAGIC El ejemplo usa el grupo `lectores_clase`. El administrador debe crearlo y agregar a la persona que hará la prueba. Quitar los `--` solo cuando corresponda aplicarlo y tengamos permiso; si solo revisamos la sintaxis, dejarlas comentadas.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GRANT USE CATALOG ON CATALOG analitica TO `lectores_clase`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA analitica.data TO `lectores_clase`;
# MAGIC -- GRANT SELECT ON VIEW analitica.data.v_ventas_por_producto TO `lectores_clase`;
# MAGIC
# MAGIC SHOW GRANTS ON VIEW v_ventas_por_producto;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 · Prueba con otra identidad
# MAGIC Una persona del grupo inicia sesión con su cuenta, abre un notebook con cómputo (como en la demo 1) y ejecuta la consulta siguiente. Si la ejecuta el propietario, solo comprueba su propio acceso.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analitica.data.v_ventas_por_producto

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 · Revisión del linaje
# MAGIC **Catalog → analitica → data → v_ventas_por_producto → Lineage** (y **See Lineage Graph** si aparece). Buscar la dependencia con `ventas`. Lo visible depende de los permisos y de las operaciones capturadas.
# MAGIC
# MAGIC Si falla el acceso, revisar el grupo y `USE CATALOG`, `USE SCHEMA` y `SELECT`. Un usuario puede recibir acceso por otras pertenencias o por herencia.
# MAGIC
# MAGIC **Comprobación de la demo 4:** distinguimos archivo, DataFrame, tabla, vista y función; identificamos la tabla base de la vista, su responsable y los permisos necesarios para consultarla.

# COMMAND ----------

resumen_vista = spark.table("v_ventas_por_producto")
total_vista = resumen_vista.agg(F.round(F.sum("importe_total"), 2)).first()[0]
total_base = spark.table("ventas").agg(F.round(F.sum(F.col("cantidad") * F.col("precio")), 2)).first()[0]

assert resumen_vista.count() == df_ventas.select("producto").distinct().count()
assert abs(total_vista - total_base) < 0.05, (total_vista, total_base)
print("Demo 4 comprobada. Importe total de la vista:", total_vista)

# COMMAND ----------

# MAGIC %md
# MAGIC ➡️ **Fin de la demo 4 · Volver a la diapositiva 56: Comprobaciones antes de continuar.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diapositiva 57 · Preguntas para revisar lo aprendido
# MAGIC Cada respuesta relaciona el comportamiento técnico con el efecto sobre los datos y va acompañada de una celda que lo demuestra. Las demostraciones usan tablas propias (`ventas_append_demo`, `ventas_merge_demo`) y no modifican las tablas de las demos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 1 · Si el CSV está en un volumen, ¿qué falta para que otro notebook consulte una tabla persistente?
# MAGIC
# MAGIC Subir el archivo al volumen solo guarda **un archivo gobernado**; no crea ninguna tabla. Falta:
# MAGIC
# MAGIC 1. **Leer** el CSV con un esquema revisado (tipos, fechas, `header`, modo `FAILFAST`) → se obtiene un DataFrame, que vive solo en la sesión de este notebook.
# MAGIC 2. **Guardarlo como tabla** registrada en Unity Catalog (`saveAsTable("ventas")` o `CREATE TABLE ... AS SELECT`). Así queda una tabla Delta con esquema, metadatos e historial en `analitica.data`.
# MAGIC 3. Que el otro notebook use un **cómputo compatible con Unity Catalog** y la consulte por su **nombre completo** `analitica.data.ventas` (no depende del catálogo/esquema activo).
# MAGIC 4. Que la identidad que consulta tenga **permisos**: `USE CATALOG`, `USE SCHEMA` y `SELECT` (ver pregunta 4).
# MAGIC
# MAGIC Efecto sobre los datos: todos leen la misma versión publicada, en lugar de que cada notebook vuelva a leer e interpretar el CSV a su manera.

# COMMAND ----------

print("¿Existe una tabla llamada como el CSV?:", spark.catalog.tableExists("analitica.data.ventas_2025"))
print("¿Existe la tabla publicada?:           ", spark.catalog.tableExists("analitica.data.ventas"))
display(spark.sql("SELECT COUNT(*) AS registros FROM analitica.data.ventas"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 2 · Si una unión duplica el importe total, ¿qué comprobación haríamos sobre las llaves?
# MAGIC
# MAGIC Una unión multiplica las filas cuando la **llave de la dimensión no es única**: cada pedido se repite tantas veces como coincidencias tenga su `product`, y al sumar `importe` se cuenta varias veces.
# MAGIC
# MAGIC Comprobaciones:
# MAGIC
# MAGIC 1. **Unicidad de la llave en la dimensión** antes de unir: `groupBy("product").count().filter("count > 1")` debe devolver cero filas.
# MAGIC 2. **Conteo antes y después**: en una unión izquierda el número de pedidos debe ser el mismo.
# MAGIC 3. **Importe total antes y después**: debe coincidir.
# MAGIC 4. **Productos sin correspondencia** con `left_anti` (los que quedarían con familia nula).
# MAGIC 5. Revisar que la llave tenga el mismo formato en ambos lados (mayúsculas, espacios, tipo).
# MAGIC
# MAGIC Si hay duplicados, se corrige la dimensión (una fila por llave) en lugar de usar `distinct()` sobre el resultado.

# COMMAND ----------

# Simulamos una dimensión con la llave "Laptop" repetida
df_productos_dup = df_productos.unionByName(df_productos.filter(F.col("product") == "Laptop"))
display(df_productos_dup.groupBy("product").count().filter("count > 1"))

df_unido_dup = df_importes.join(df_productos_dup, "product", "left")
importe_correcto = df_importes.agg(F.sum("importe")).first()[0]
importe_duplicado = df_unido_dup.agg(F.sum("importe")).first()[0]

print("Pedidos antes / después:", df_importes.count(), "/", df_unido_dup.count())
print("Importe antes / después:", importe_correcto, "/", importe_duplicado)
assert df_unido_dup.count() > df_importes.count() and importe_duplicado > importe_correcto

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 3 · Si una celda define un filtro, ¿significa que Spark ya calculó todo el resultado?
# MAGIC
# MAGIC **No.** `filter`, `select`, `withColumn`, `join` o `groupBy` son **transformaciones**: solo describen el resultado y Spark añade ese paso a un **plan lógico** (evaluación diferida). No se leen los archivos ni se filtran filas en ese momento.
# MAGIC
# MAGIC El cálculo ocurre cuando se ejecuta una **acción**: `count()`, `display()`, `show()`, `collect()` o escribir una tabla. Entonces el optimizador combina los pasos (por ejemplo, empuja el filtro hacia la lectura) y el driver reparte tareas a los executors.
# MAGIC
# MAGIC Efecto sobre los datos: cada acción vuelve a ejecutar el plan contra la fuente; si el archivo cambió entre dos acciones, los resultados pueden ser diferentes. Por eso guardamos el resultado como tabla cuando necesitamos un resultado estable.

# COMMAND ----------

df_filtro = df_pedidos.filter(F.col("country") == "Spain")  # transformación: solo define el plan
print(type(df_filtro).__name__, "→ todavía no hay resultado calculado")
df_filtro.explain()  # muestra el plan que Spark ejecutaría

print("Acción count() → filas calculadas:", df_filtro.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 4 · Si una persona tiene SELECT sobre una tabla, ¿qué otros permisos de uso necesita?
# MAGIC
# MAGIC `SELECT` autoriza leer el objeto, pero en Unity Catalog también hay que poder **usar sus contenedores**:
# MAGIC
# MAGIC | Ámbito | Privilegio |
# MAGIC |---|---|
# MAGIC | Catálogo `analitica` | `USE CATALOG` |
# MAGIC | Esquema `analitica.data` | `USE SCHEMA` |
# MAGIC | Tabla o vista | `SELECT` |
# MAGIC
# MAGIC Además necesita acceso al **workspace** y permiso para usar un **cómputo** (por ejemplo `CAN ATTACH TO` en un clúster clásico, o Serverless/SQL warehouse habilitado). Si va a leer los CSV del volumen, requiere `READ VOLUME` (un `SELECT` sobre la tabla no da acceso a archivos). Lo recomendable es conceder a un **grupo** (`lectores_clase`) y comprobar el acceso con una identidad de ese grupo, no con la del propietario.
# MAGIC
# MAGIC ```sql
# MAGIC GRANT USE CATALOG ON CATALOG analitica TO `lectores_clase`;
# MAGIC GRANT USE SCHEMA ON SCHEMA analitica.data TO `lectores_clase`;
# MAGIC GRANT SELECT ON TABLE analitica.data.ventas TO `lectores_clase`;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON CATALOG analitica

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON SCHEMA analitica.data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 5 · Si el historial de la tabla muestra una versión antigua, ¿basta para garantizar su recuperación?
# MAGIC
# MAGIC **No.** `DESCRIBE HISTORY` lee el registro de transacciones (`_delta_log`) y muestra que la versión **existió**, pero leerla con Time Travel (`versionAsOf`) o restaurarla (`RESTORE`) requiere que sigan disponibles **los archivos de datos** de esa versión **y** el registro necesario.
# MAGIC
# MAGIC - `VACUUM` elimina los archivos que ya no pertenecen a la versión actual y superan `delta.deletedFileRetentionDuration` (habitualmente **7 días**).
# MAGIC - El registro se conserva según `delta.logRetentionDuration` (habitualmente **30 días**).
# MAGIC - La política efectiva depende del entorno y del mantenimiento de la tabla (por ejemplo, la optimización predictiva puede ejecutar `VACUUM`).
# MAGIC
# MAGIC Efecto sobre los datos: una versión puede aparecer en el historial y fallar al leerla. Time Travel no reemplaza una copia de respaldo; para garantizar la recuperación se define la retención y se comprueba leyendo la versión. En la práctica no ejecutamos `VACUUM` ni cambiamos la retención.

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY ventas_delta_demo").select("version", "timestamp", "operation"))
display(spark.sql("SHOW TBLPROPERTIES ventas_delta_demo"))

# La prueba real de recuperación es leer la versión, no solo verla en el historial
print("Filas en la versión", version_inicial, ":",
      spark.read.option("versionAsOf", version_inicial).table("ventas_delta_demo").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pregunta 6 · Si repetimos un append con el mismo archivo, ¿qué puede pasar con las ventas?
# MAGIC
# MAGIC `mode("append")` (o `INSERT INTO`) **agrega filas sin revisar si ya existen**: Delta registra la escritura como una transacción válida, aunque los datos estén repetidos. Al cargar dos veces `ventas_2025.csv` la tabla pasa de 100 a **200 filas**, cada `id` aparece dos veces y las **unidades y el importe se duplican**, sin que ninguna operación falle.
# MAGIC
# MAGIC Cómo evitarlo:
# MAGIC
# MAGIC - Cargar con **`MERGE` por la llave `id`** (idempotente: repetir la carga no cambia el resultado), o `overwrite` cuando la tabla representa una carga completa.
# MAGIC - Comprobar **llaves repetidas y conteos** después de cada carga.
# MAGIC - Registrar qué archivos ya se cargaron (lo trabajaremos en las sesiones 3 y 4 con la ingesta incremental).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ventas_append_demo (
# MAGIC   id INT, fecha DATE, producto STRING, cantidad INT, precio DOUBLE
# MAGIC ) USING DELTA

# COMMAND ----------

for carga in (1, 2):  # el mismo archivo, dos veces
    df_ventas.write.format("delta").mode("append").saveAsTable("ventas_append_demo")

df_append = spark.table("ventas_append_demo")
importe = lambda df: df.agg(F.round(F.sum(F.col("cantidad") * F.col("precio")), 2)).first()[0]

print("Filas en el CSV / en la tabla:", df_ventas.count(), "/", df_append.count())
print("Ids distintos:", df_append.select("id").distinct().count())
print("Importe original / tras dos append:", importe(df_ventas), "/", importe(df_append))
display(df_append.groupBy("id").count().filter("count > 1").orderBy("id").limit(5))
assert df_append.count() == 2 * df_ventas.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Alternativa idempotente: la misma carga, repetida dos veces con `MERGE` por `id`, deja exactamente 100 filas.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ventas_merge_demo (
# MAGIC   id INT, fecha DATE, producto STRING, cantidad INT, precio DOUBLE
# MAGIC ) USING DELTA

# COMMAND ----------

df_ventas.createOrReplaceTempView("carga_ventas")

for carga in (1, 2):  # el mismo archivo, dos veces
    spark.sql("""
        MERGE INTO ventas_merge_demo AS destino
        USING carga_ventas AS fuente
        ON destino.id = fuente.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

df_merge = spark.table("ventas_merge_demo")
print("Filas tras dos MERGE:", df_merge.count())
print("Importe tras dos MERGE:", importe(df_merge))
assert df_merge.count() == df_ventas.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ➡️ **Fin de las preguntas · Continuar en la diapositiva 58: Continuidad con las sesiones 3 y 4.**
