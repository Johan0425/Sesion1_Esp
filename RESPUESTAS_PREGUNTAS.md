# Diapositiva 57 · Preguntas para revisar lo aprendido

**Sesión 2 · Spark, Delta Lake y gobernanza** — Arquitecturas ETL y Modelos de IA Aplicados · EAM · 2026-II

Cada respuesta relaciona el comportamiento técnico con el efecto sobre los datos. Las mismas respuestas están al final del notebook [`01_Fundamentos_Databricks.py`](01_Fundamentos_Databricks.py), cada una con una celda que la demuestra. Los resultados citados salen de esas celdas (ejecución local con Spark 3.5.3 + Delta 3.2.1, ver [README](README.md#pruebas)).

---

## 1. Si el CSV está en un volumen, ¿qué falta para que otro notebook consulte una tabla persistente?

Subir el archivo al volumen `landing` solo guarda **un archivo gobernado**; **no crea ninguna tabla**. Falta:

1. **Leer** el CSV con un esquema revisado (tipos, fechas, `header`, modo `FAILFAST`). Así se obtiene un DataFrame, que vive solo en la sesión del notebook que lo creó.
2. **Guardarlo como tabla** registrada en Unity Catalog: `df_ventas.write.format("delta").mode("overwrite").saveAsTable("ventas")` o `CREATE TABLE ... AS SELECT`. Queda una tabla Delta con esquema, metadatos e historial dentro de `analitica.data`.
3. Que el otro notebook use un **cómputo compatible con Unity Catalog** y consulte la tabla por su **nombre completo**, `analitica.data.ventas`, para no depender del catálogo y el esquema activos.
4. Que la identidad que consulta tenga **permisos**: `USE CATALOG`, `USE SCHEMA` y `SELECT` (ver pregunta 4).

**Efecto sobre los datos:** todos leen la misma versión publicada, en lugar de que cada notebook vuelva a leer e interpretar el CSV a su manera.

**Evidencia en el notebook:** `spark.catalog.tableExists("analitica.data.ventas_2025")` devuelve `False` (el archivo no es una tabla) y `analitica.data.ventas` devuelve `True`, con 100 registros.

---

## 2. Si una unión duplica el importe total, ¿qué comprobación haríamos sobre las llaves?

Una unión multiplica filas cuando **la llave de la dimensión no es única**: cada pedido se repite tantas veces como coincidencias tenga su `product`, y al sumar `importe` ese pedido se cuenta varias veces.

Comprobaciones:

1. **Unicidad de la llave en la dimensión** antes de unir: `df_productos.groupBy("product").count().filter("count > 1")` debe devolver cero filas.
2. **Conteo antes y después:** en una unión izquierda el número de pedidos no debe cambiar (`assert df_enriquecido.count() == df_pedidos.count()`).
3. **Importe total antes y después:** debe coincidir.
4. **Llaves sin correspondencia** con `left_anti`: productos que quedarían con familia y proveedor nulos.
5. **Mismo formato de la llave** en ambos lados: mayúsculas, espacios, tipo de dato.

Si hay duplicados, se corrige la dimensión para que tenga una fila por llave. Aplicar `distinct()` al resultado solo esconde el problema.

**Evidencia en el notebook:** con `Laptop` repetida en la dimensión, los pedidos pasan de **10 a 13** y el importe de **6.550 a 11.400**.

---

## 3. Si una celda define un filtro, ¿significa que Spark ya calculó todo el resultado?

**No.** `filter`, `select`, `withColumn`, `join` y `groupBy` son **transformaciones**: describen el resultado, y Spark agrega ese paso a un **plan lógico** (evaluación diferida). En ese momento todavía no lee los archivos ni filtra filas.

El cálculo ocurre con una **acción**: `count()`, `display()`, `show()`, `collect()` o la escritura de una tabla. Entonces el optimizador combina los pasos (por ejemplo, empuja el filtro hacia la lectura del CSV), y el driver reparte tareas sobre particiones a los executors.

**Efecto sobre los datos:** cada acción vuelve a ejecutar el plan contra la fuente. Si el archivo cambió entre dos acciones, los resultados pueden ser distintos. Por eso guardamos el resultado como tabla cuando necesitamos un resultado estable.

**Evidencia en el notebook:** `df_filtro = df_pedidos.filter(...)` solo devuelve un `DataFrame`; `df_filtro.explain()` muestra el plan (con el filtro aplicado sobre la lectura del CSV) y solo `df_filtro.count()` calcula las **4** filas de Spain.

---

## 4. Si una persona tiene SELECT sobre una tabla, ¿qué otros permisos de uso necesita?

`SELECT` autoriza leer el objeto, pero en Unity Catalog la persona también debe poder **usar sus contenedores**:

| Ámbito | Privilegio | Qué permite |
|---|---|---|
| Catálogo `analitica` | `USE CATALOG` | Usar el catálogo como contenedor |
| Esquema `analitica.data` | `USE SCHEMA` | Usar el esquema donde está el objeto |
| Tabla o vista | `SELECT` | Consultar los datos |

Además necesita acceso al **workspace** y permiso para usar un **cómputo**: `CAN ATTACH TO` en un clúster clásico, o Serverless / SQL warehouse habilitado. Para leer los CSV del volumen necesita `READ VOLUME`, porque `SELECT` sobre la tabla no da acceso a los archivos.

Lo recomendable es conceder los permisos a un **grupo** (`lectores_clase`) y comprobarlos con una identidad de ese grupo. Si la prueba la hace el propietario, solo demuestra su propio acceso. También hay que revisar concesiones heredadas o recibidas por otros grupos.

```sql
GRANT USE CATALOG ON CATALOG analitica TO `lectores_clase`;
GRANT USE SCHEMA ON SCHEMA analitica.data TO `lectores_clase`;
GRANT SELECT ON TABLE analitica.data.ventas TO `lectores_clase`;
```

**Evidencia en el notebook:** `SHOW GRANTS ON CATALOG analitica`, `SHOW GRANTS ON SCHEMA analitica.data` y `SHOW GRANTS ON VIEW v_ventas_por_producto`.

---

## 5. Si el historial de la tabla muestra una versión antigua, ¿basta para garantizar su recuperación?

**No.** `DESCRIBE HISTORY` lee el registro de transacciones (`_delta_log`) y muestra que la versión **existió**. Para leerla con Time Travel (`versionAsOf`) o restaurarla con `RESTORE`, tienen que seguir disponibles **los archivos de datos** de esa versión **y** el registro necesario.

- `VACUUM` borra los archivos que ya no forman parte de la versión actual cuando superan `delta.deletedFileRetentionDuration` (habitualmente **7 días**).
- El registro se conserva según `delta.logRetentionDuration` (habitualmente **30 días**).
- La política efectiva depende del entorno y del mantenimiento de la tabla; por ejemplo, la optimización predictiva puede ejecutar `VACUUM` automáticamente.

**Efecto sobre los datos:** una versión puede seguir apareciendo en el historial y aun así fallar al leerla. Time Travel no reemplaza un respaldo. Para garantizar la recuperación se define la retención y se comprueba **leyendo** la versión. En la práctica no ejecutamos `VACUUM` ni cambiamos la retención.

**Evidencia en el notebook:** `DESCRIBE HISTORY` y `SHOW TBLPROPERTIES ventas_delta_demo`, y una lectura real de la versión de referencia (100 filas).

---

## 6. Si repetimos un append con el mismo archivo, ¿qué puede pasar con las ventas?

`mode("append")` (o `INSERT INTO`) **agrega filas sin revisar si ya existen**. Delta registra cada escritura como una transacción válida aunque los datos estén repetidos: ACID protege la escritura, no el significado. Al cargar dos veces `ventas_2025.csv`:

- la tabla pasa de **100 a 200 filas**;
- cada `id` aparece **dos veces**;
- las unidades y el importe se **duplican** (**52.313,58 → 104.627,16**);

y ninguna operación falla.

Cómo evitarlo:

- Cargar con **`MERGE` por la llave `id`**, que es idempotente (repetir la carga no cambia el resultado), o usar `overwrite` cuando la tabla representa una carga completa.
- Comprobar **llaves repetidas y conteos** después de cada carga.
- Registrar qué archivos ya se cargaron (lo veremos en las sesiones 3 y 4 con la ingesta incremental).

**Evidencia en el notebook:** `ventas_append_demo` queda con 200 filas tras dos `append`; `ventas_merge_demo` queda con **100** filas y el importe original tras dos `MERGE` del mismo archivo.
