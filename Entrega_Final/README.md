# Entrega final · Trabajo final de la aerolínea

**Arquitecturas ETL y Modelos de IA Aplicados · EAM · 2026-II**

Pipeline Bronze → Silver → Gold sobre 50.000 vuelos (`flights.csv`, `airlines.csv`, `airports.csv`) con Spark y Delta Lake, análisis con funciones de ventana, KPI de negocio optimizados, un **Job de 3 tareas** con dependencias, reintentos y correo, y un **modelo de regresión** del retraso de llegada registrado en MLflow.

## Qué pide el trabajo y dónde está

| Apartado / entregable | Archivo |
|---|---|
| 1 · Ingestión y limpieza (Bronze) | [`notebooks/01_Pipeline_Vuelos_Apartados_1-4`](notebooks/01_Pipeline_Vuelos_Apartados_1-4.py) · secciones 0 y 1 (con resultados: [`notebooks_resultados/`](notebooks_resultados)) |
| 2 · Transformaciones y modelado (Silver) | mismo notebook · sección 2 |
| 3 · Análisis avanzado y métricas | mismo notebook · sección 3 |
| 4 · Capa Gold, `OPTIMIZE` / `ZORDER BY` | mismo notebook · sección 4 y conclusiones de negocio |
| 5 · Ficheros `.py` de bronce, plata y oro | [`pipeline/bronze_ingestion.py`](pipeline/bronze_ingestion.py) · [`silver_transform.py`](pipeline/silver_transform.py) · [`gold_analytics.py`](pipeline/gold_analytics.py) |
| 5 · `.yaml` del Job | [`pipeline/job/trabajo_final_job.yaml`](pipeline/job/trabajo_final_job.yaml), exportado del Job real `Trabajo_Final_Vuelos` |
| 6 · Machine learning | [`notebooks/02_ML_Retraso_Llegada`](notebooks/02_ML_Retraso_Llegada.py), con resultados en [`notebooks_resultados/`](notebooks_resultados), y [`resultados_ml/`](resultados_ml): métricas, 8.255 predicciones y figura de la ejecución en Databricks |
| Capturas de Unity Catalog, del Job y del flujo ejecutado | [`capturas/`](capturas) (**las tomas tú**, ver la lista) |

Los notebooks se guardan en `.py` (formato fuente de Databricks, se abre directo desde un Git folder). [`notebooks_resultados/`](notebooks_resultados) contiene la ejecución en Databricks Serverless de cada uno, con todas sus salidas, en `.ipynb` y en `.html` (la exportación de Databricks, se abre en el navegador). No se guardan junto a los `.py` porque Databricks trataría los dos archivos como el mismo notebook y el Git folder no se clonaría completo.

**Estado en Databricks (5-nov-2026):** los dos notebooks y el Job `Trabajo_Final_Vuelos` se ejecutaron en Serverless con resultado *Succeeded* (Job: 3 tareas al primer intento, ~30 s cada una). La ejecución de MLflow `arbol_retraso_llegada` tiene el run ID `706f29f92bb14971a35ab64884fcbe2c`. **Solo faltan las capturas y el zip.**

## Organización en Unity Catalog

```
aerolinea_tf                         ← catálogo propio del trabajo
├── fuentes   · volumen landing      ← flights.csv, airlines.csv, airports.csv
├── bronze    · flights, airlines, airports
├── silver    · flights_enriched (particionada por mes), airline_metrics,
│               airline_origin_ontime, airport_metrics
├── analisis  · delay_by_month / _weekday / _quarter, top5_delayed_flights_row_number,
│               top5_delayed_flights_dense_rank, ranking_comparison,
│               airport_delay_rank, airline_month_metrics
├── gold      · delay_airline_airport_month, airport_punctuality_ranking,
│               airline_ontime_kpi, ml_flights
└── ml        · metricas_retraso, predicciones_retraso, volumen modelos
```

Cada esquema tiene un comentario que explica a qué apartado pertenece.

## Decisiones principales

- **Bronze:** se lee todo como texto con `FAILFAST` y se convierte con `try_cast`. Los retrasos llegan como `"25.0"` y pasan a `int` sin perder información. Se descartan **521 vuelos sin `flight_id`** (no había nulos en `airline_id` ni en `origin_airport`); quedan **49.479**.
- **Silver:** `on_time` = `arrival_delay ≤ 15` min. `delay_category` se calcula sobre `arrival_delay`: None ≤ 0 · Short 1–15 · Medium 16–45 · Long > 45 · Unknown sin dato. `distance_km` = millas × 1,609344. Los **473 vuelos sin alguno de los retrasos no se rellenan**: un 0 inventaría vuelos puntuales. Se marcan con `delay_data_complete`.
- **Gold:** cada tabla reconcilia con Silver (las sumas de vuelos y de vuelos puntuales coinciden). `OPTIMIZE ... ZORDER BY` se aplica sobre las columnas de filtro de los dashboards.
- **ML:** se predice el retraso de llegada **al despegar**, con la aerolínea, la ruta, la distancia, el día de la semana y el retraso de salida. Se entrena con enero–mayo y se evalúa con junio de 2025. El árbol baja el **MAE de 18,31 a 8,01 min** (−56 %) frente a la referencia del promedio. Un segundo árbol **sin el retraso de salida** apenas mejora (18,08): antes del despegue casi no se puede anticipar.

---

## Pasos en Databricks (lo que falta hacer tú)

### 1. Subir el código
**Workspace → Create → Git folder** con la URL de este repositorio (después de hacer `git push`). Si no, **Workspace → Import** de los dos notebooks `.py` y de los tres `.py` de `pipeline/`.

### 2. Datos
Si usas el Git folder, el notebook y la tarea Bronze copian solos los CSV de `datos/` al volumen. Si importaste los archivos, sube los tres CSV a **Catalog → aerolinea_tf → fuentes → landing → Upload to this volume** después de ejecutar la celda 0.1.

### 3. Notebook de los apartados 1 a 4
Abrir `notebooks/01_Pipeline_Vuelos_Apartados_1-4`, conectar a **Serverless** y **Run all**. Todas las celdas deben terminar sin error; los `assert` comprueban los conteos (49.479 vuelos, 360 filas en la tabla mensual, etc.).

### 4. Job del apartado 5
**Jobs & Pipelines → Create → Job**, con nombre `Trabajo_Final_Vuelos`:

| Tarea | Tipo | Archivo | Depends on |
|---|---|---|---|
| `Bronze_Ingestion` | Python script · Workspace | `pipeline/bronze_ingestion.py` | — |
| `Silver_Transform` | Python script · Workspace | `pipeline/silver_transform.py` | Bronze_Ingestion |
| `Gold_Analytics` | Python script · Workspace | `pipeline/gold_analytics.py` | Silver_Transform |

- En **cada tarea**: cómputo **Serverless**, **Retries = 2** con 1 minuto entre intentos y *Retry on timeout*.
- En el panel del Job: **Job notifications → Add → tu correo** para *Success* y *Failure*.
- **Schedules & Triggers → Add trigger → Scheduled**: diario a las 06:00 (America/Bogota), para que los datos se actualicen sin intervención manual.
- **Run now** y esperar a que las 3 tareas queden en verde (*Succeeded*).

### 5. Exportar el `.yaml`
En el Job: **(⋮) → View as code / Edit as YAML → YAML → Copy**. Pegar el contenido en `pipeline/job/trabajo_final_job.yaml`, reemplazando el de referencia. El exportado lleva tus rutas y tu correo reales.

### 6. Notebook de machine learning
Abrir `notebooks/02_ML_Retraso_Llegada`. En **Environment**, elegir **Standard** (versión 6 si aparece) y **Apply**. **Run all**. Al final aparecen el `Run ID`, las tablas `aerolinea_tf.ml.*` y el experimento en **Experiments**.

### 7. Capturas
Seguir la lista de [`capturas/README.md`](capturas/README.md) y guardar las imágenes en `capturas/`.

### 8. Exportar los notebooks con resultados
En cada notebook: **File → Export → IPython Notebook**. Guardar los dos `.ipynb` en `notebooks_resultados/` para que el profesor vea los resultados.

### 9. Empaquetar
Desde la carpeta `Sesion1_Esp`:

```bash
zip -r Entrega_Final.zip Entrega_Final -x "*/__pycache__/*" "*.DS_Store"
```

---

## Prueba local

[`tests/run_local.py`](tests/run_local.py) ejecuta todo en Spark 3.5.3 + Delta Lake 3.2.1 con ANSI activado, como Serverless:

1. El notebook de los apartados 1 a 4.
2. Las 3 tareas del Job desde cero, cada una en un contexto nuevo. Al terminar, comprueba que **las 11 tablas Bronze/Silver/Gold son idénticas** a las del notebook.
3. El notebook de ML. Exporta `resultados_ml/metricas.csv`, `predicciones_evaluacion.csv` y `evaluacion_retraso_llegada.png`.

```bash
pip install pyspark==3.5.3 delta-spark==3.2.1 pandas scikit-learn matplotlib joblib mlflow-skinny   # Java 17
python tests/run_local.py
```

Último resultado: **5 tareas OK** (notebook 1-4: 31 celdas · 3 scripts · ML: 15 celdas), las 11 tablas coinciden y todos los `assert` se cumplen. En local se omiten `CREATE CATALOG` y `CREATE VOLUME`, que solo existen en Unity Catalog.
