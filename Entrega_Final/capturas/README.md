# Capturas de la entrega

Tomadas en el workspace de Databricks Free Edition el 5 de noviembre de 2026.

## Unity Catalog (orden del proyecto)

| Archivo | Qué muestra |
|---|---|
| `01_catalogo_esquemas.png` | Catálogo `aerolinea_tf` con sus esquemas: `fuentes`, `bronze`, `silver`, `analisis`, `gold` y `ml`, cada uno con el comentario del apartado al que pertenece |
| `02_volumen_landing.png` | Volumen `aerolinea_tf.fuentes.landing` con los 3 CSV de origen |
| `03_tablas_bronze.png` | Esquema `bronze`: `flights`, `airlines`, `airports` |
| `04_tablas_silver.png` | Esquema `silver`: `flights_enriched` y las tres tablas de métricas |
| `05_tablas_analisis.png` | Esquema `analisis`: las 8 tablas del apartado 3 |
| `06_tablas_gold.png` | Esquema `gold`: las 4 tablas del apartado 4 |
| `07_esquema_ml.png` | Esquema `ml`: `metricas_retraso`, `predicciones_retraso` y el volumen `modelos` |
| `08_gold_sample_data.png` | `gold.airline_ontime_kpi` en **Sample Data**: KPI de puntualidad de las 6 aerolíneas |
| `09_history_gold.png` | **History** de una tabla Gold: versiones escritas por el notebook y por el Job, y `SET TBLPROPERTIES` con la optimización automática. `OPTIMIZE` no crea versión porque cada tabla ya ocupa un solo archivo; sus métricas están en la sección 4.2 del notebook |

## Job y flujo completo

| Archivo | Qué muestra |
|---|---|
| `10_job_tareas_dependencias.png` | Job `Trabajo_Final_Vuelos`: `Bronze_Ingestion → Silver_Transform → Gold_Analytics` |
| `11_job_reintentos.png` | Tarea `Bronze_Ingestion`: reintentos (1 min de espera, hasta 3 intentos, reintento por timeout) y timeout de 20 min |
| `12_job_notificaciones.png` | Notificaciones por correo *On success* y *On failure* |
| `13_job_schedule.png` | Programación diaria a las 06:00 (America/Bogota) |
| `14_job_runs.png` | Historial de ejecuciones: las 3 tareas en verde |
| `15_job_run_exitoso.png` | Detalle de la ejecución: las 3 tareas *Succeeded* (34 s, 37 s y 35 s) |

## Machine learning

| Archivo | Qué muestra |
|---|---|
| `16_mlflow_run_metricas.png` | Ejecución `arbol_retraso_llegada` en MLflow: métricas (MAE 8,01 frente a 18,31), parámetros y artefactos |
| `17_mlflow_artefactos.png` | Artefactos de la ejecución: modelo `.joblib`, figura de evaluación y CSV |
