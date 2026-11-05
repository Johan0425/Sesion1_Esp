# Entrega final · Trabajo final de la aerolínea

**Arquitecturas ETL y Modelos de IA Aplicados · EAM · 2026-II**

Pipeline Bronze → Silver → Gold sobre 50.000 vuelos (`flights.csv`, `airlines.csv`, `airports.csv`) con Spark y Delta Lake. Incluye análisis con funciones de ventana, KPI de negocio optimizados, un **Job de 3 tareas** con dependencias, reintentos, notificaciones y programación diaria, y un **modelo de regresión** del retraso de llegada registrado en MLflow.

Todo se ejecutó en **Databricks Free Edition (Serverless)** el 5 de noviembre de 2026, con resultado *Succeeded*.

## Entregables

| Apartado / entregable | Dónde está |
|---|---|
| 1 · Ingestión y limpieza (Bronze) | [`notebooks/01_Pipeline_Vuelos_Apartados_1-4`](notebooks/01_Pipeline_Vuelos_Apartados_1-4.py) · secciones 0 y 1 |
| 2 · Transformaciones y modelado (Silver) | mismo notebook · sección 2 |
| 3 · Análisis avanzado y métricas | mismo notebook · sección 3 |
| 4 · Capa Gold y optimización Delta | mismo notebook · sección 4 y conclusiones de negocio |
| 5 · Ficheros `.py` de bronce, plata y oro | [`pipeline/bronze_ingestion.py`](pipeline/bronze_ingestion.py) · [`silver_transform.py`](pipeline/silver_transform.py) · [`gold_analytics.py`](pipeline/gold_analytics.py) |
| 5 · `.yaml` del Job | [`pipeline/job/trabajo_final_job.yaml`](pipeline/job/trabajo_final_job.yaml), exportado del Job `Trabajo_Final_Vuelos` |
| 6 · Machine learning | [`notebooks/02_ML_Retraso_Llegada`](notebooks/02_ML_Retraso_Llegada.py) y [`resultados_ml/`](resultados_ml) (métricas, 8.255 predicciones y figura) |
| Notebooks con resultados | [`notebooks_resultados/`](notebooks_resultados): la ejecución en Databricks de los dos notebooks con todas sus salidas, en `.ipynb` y `.html` |
| Capturas de Unity Catalog, del Job y de MLflow | [`capturas/`](capturas): 17 capturas descritas en [`capturas/README.md`](capturas/README.md) |

Los notebooks de `notebooks/` están en formato fuente de Databricks (`.py`), que se abre directamente desde un Git folder. Las versiones ejecutadas están en una carpeta aparte (`notebooks_resultados/`) porque Databricks trataría un `.py` y un `.ipynb` con el mismo nombre como el mismo notebook.

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

Hay un esquema por etapa del pipeline, y cada uno lleva un comentario con el apartado al que corresponde (captura `01_catalogo_esquemas.png`).

## Decisiones principales

- **Bronze:** se lee todo como texto con `FAILFAST` y se convierte con `try_cast`. Los retrasos llegan como `"25.0"` y pasan a `int` sin perder información. Se descartan **521 vuelos sin `flight_id`** (no había nulos en `airline_id` ni en `origin_airport`); quedan **49.479**.
- **Silver:** `on_time` = `arrival_delay ≤ 15` min. `delay_category` se calcula sobre `arrival_delay`: None ≤ 0 · Short 1–15 · Medium 16–45 · Long > 45 · Unknown sin dato. `distance_km` = millas × 1,609344. Los **473 vuelos sin alguno de los retrasos no se rellenan**, porque un 0 inventaría vuelos puntuales; se marcan con `delay_data_complete`.
- **Análisis:** `row_number` devuelve exactamente 5 vuelos por aerolínea (30 en total). `dense_rank` devuelve 36, porque incluye los empates en el retraso total.
- **Gold:** cada tabla reconcilia con Silver: las sumas de vuelos y de vuelos puntuales coinciden. Se activa la compactación automática (`delta.autoOptimize.*`) y se ejecuta `OPTIMIZE ... ZORDER BY` sobre las columnas de filtro de los dashboards. Con este volumen, en Databricks cada tabla ya ocupa un solo archivo, así que `OPTIMIZE` revisa los archivos sin necesidad de compactar. El notebook muestra esas métricas y el historial de Delta registra `SET TBLPROPERTIES` (captura `09_history_gold.png`).
- **Job:** las tres tareas se ejecutan en cadena (`Bronze_Ingestion → Silver_Transform → Gold_Analytics`). Cada una tiene 2 reintentos con 1 minuto de espera y reintento por timeout. Hay notificación por correo en caso de éxito o fallo y una ejecución programada a diario a las 06:00 (America/Bogota). Los tres scripts producen las mismas tablas que el notebook.
- **ML:** se predice el retraso de llegada **al despegar**, con la aerolínea, la ruta, la distancia, el día de la semana y el retraso de salida. Se entrena con enero–mayo y se evalúa con junio de 2025. Las variables calculadas a partir del retraso de llegada se excluyen para evitar fuga de información.

## Resultados

**Negocio**
- La puntualidad de la red es del 53,99 %, con un retraso de llegada medio de 13,4 min.
- United, Delta, American y Southwest llegan a tiempo entre el 58,8 % y el 60,1 % de las veces. **JetBlue (42,4 %) y Spirit (42,8 %) quedan unos 11 puntos por debajo**, y la brecha se repite en todos los aeropuertos y meses.
- Entre aeropuertos (53,1 %–54,7 %), meses y días de la semana las diferencias son mínimas.

**Machine learning** (evaluación con 8.255 vuelos de junio)

| Modelo | MAE (min) | RMSE (min) |
|---|---|---|
| Referencia: promedio | 18,31 | 23,00 |
| **Árbol al despegar** | **8,01** | **10,03** |
| Árbol antes de salir (sin retraso de salida) | 18,08 | 22,64 |

El árbol reduce el MAE un 56 % frente a la referencia, sin sobreajuste (MAE de entrenamiento 7,96). La mejora viene del retraso de salida. Sin él, el modelo apenas supera el promedio: antes del despegue casi no se puede anticipar el retraso con estos datos. La interpretación completa está en la sección 6.11 del notebook.

**Evidencias de ejecución**
- Job `Trabajo_Final_Vuelos`: 3 tareas *Succeeded* al primer intento, unos 35 s cada una (capturas 10 a 15).
- Ejecución de MLflow `arbol_retraso_llegada`, run ID `706f29f92bb14971a35ab64884fcbe2c`, con el modelo, la figura y los CSV como artefactos (capturas 16 y 17).

## Cómo reproducirlo

1. **Workspace → Create → Git folder** con la URL de este repositorio.
2. Abrir `notebooks/01_Pipeline_Vuelos_Apartados_1-4` en Serverless y ejecutar **Run all**. El notebook crea el catálogo, los esquemas y el volumen, y copia los CSV de `datos/` al volumen. Los `assert` comprueban los conteos.
3. Crear el Job con [`pipeline/job/trabajo_final_job.yaml`](pipeline/job/trabajo_final_job.yaml), ajustando la ruta del usuario y el correo, y ejecutar **Run now**.
4. Abrir `notebooks/02_ML_Retraso_Llegada` con el entorno **Standard** y ejecutar **Run all**.

### Prueba local

[`tests/run_local.py`](tests/run_local.py) ejecuta toda la entrega en Spark 3.5.3 + Delta Lake 3.2.1 con ANSI activado, como Serverless:

1. El notebook de los apartados 1 a 4.
2. Las 3 tareas del Job desde cero, cada una en un contexto nuevo, y comprueba que **las 11 tablas Bronze/Silver/Gold son idénticas** a las del notebook.
3. El notebook de ML.

```bash
pip install pyspark==3.5.3 delta-spark==3.2.1 pandas scikit-learn matplotlib joblib mlflow-skinny   # Java 17
python tests/run_local.py
```

Último resultado: las 5 ejecuciones terminan bien, las 11 tablas coinciden y se cumplen todos los `assert`. En local se omiten `CREATE CATALOG` y `CREATE VOLUME`, que solo existen en Unity Catalog.
