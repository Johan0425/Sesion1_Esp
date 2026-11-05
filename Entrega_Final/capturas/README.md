# Capturas para la entrega

Guardar cada imagen en esta carpeta con el nombre indicado (PNG). En macOS: **⌘ + Shift + 4** y arrastrar sobre la zona.

## Unity Catalog (orden del proyecto)

| Archivo | Qué mostrar |
|---|---|
| `01_catalogo_esquemas.png` | **Catalog → aerolinea_tf** desplegado: los 6 esquemas `fuentes`, `bronze`, `silver`, `analisis`, `gold`, `ml` |
| `02_volumen_landing.png` | **aerolinea_tf → fuentes → Volumes → landing** con los 3 CSV |
| `03_tablas_bronze_silver.png` | Esquemas `bronze` y `silver` desplegados con sus tablas |
| `04_tablas_analisis_gold_ml.png` | Esquemas `analisis`, `gold` y `ml` desplegados con sus tablas (y el volumen `modelos`) |
| `05_gold_sample_data.png` | Una tabla Gold abierta (p. ej. `airline_ontime_kpi`) en la pestaña **Sample Data** |
| `06_history_optimize.png` | Pestaña **History** de `gold.delay_airline_airport_month` con la operación `OPTIMIZE` |

## Job y flujo completo

| Archivo | Qué mostrar |
|---|---|
| `07_job_tareas_dependencias.png` | Pestaña **Tasks** del Job con el grafo `Bronze_Ingestion → Silver_Transform → Gold_Analytics` |
| `08_job_reintentos.png` | Panel de una tarea con **Retries** configurados |
| `09_job_notificaciones_schedule.png` | Panel derecho del Job: **Job notifications** (correo) y **Schedules & Triggers** |
| `10_job_run_exitoso.png` | Pestaña **Runs**: la ejecución con las **3 tareas en verde** (*Succeeded*) |
| `11_job_run_detalle.png` | Detalle de la ejecución (vista de grafo o timeline) con la duración de cada tarea |

## Machine learning

| Archivo | Qué mostrar |
|---|---|
| `12_mlflow_experimento.png` | **Experiments** → la ejecución `arbol_retraso_llegada` con sus métricas y parámetros |
| `13_mlflow_artefactos.png` | Pestaña **Artifacts** de la ejecución (modelo, figura, CSV) |
