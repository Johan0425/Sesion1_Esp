# Databricks notebook source
# MAGIC %md
# MAGIC # Trabajo final · Apartado 6 · Machine learning
# MAGIC **Arquitecturas ETL y Modelos de IA Aplicados · EAM · 2026-II**
# MAGIC
# MAGIC Estimamos el **retraso de llegada** (`arrival_delay`, en minutos) de cada vuelo con una referencia basada en el promedio y un árbol de regresión, siguiendo el proceso de la sesión 5.
# MAGIC
# MAGIC | Elemento | Definición |
# MAGIC |---|---|
# MAGIC | Unidad de observación | Un vuelo (`flight_id`) |
# MAGIC | Objetivo | `arrival_delay` en minutos (negativo = llegó antes de hora) |
# MAGIC | Momento de la predicción | **Cuando el avión despega**: ya se conocen la aerolínea, la ruta, la distancia, el día y el **retraso de salida**; el retraso de llegada todavía no |
# MAGIC | Uso | Avisar a la puerta de llegada, conexiones y tripulaciones del retraso esperado |
# MAGIC | Entrada | `aerolinea_tf.gold.ml_flights` (apartado 4: vuelos con ambos retrasos conocidos) |
# MAGIC | Salidas | Esquema `aerolinea_tf.ml`: tablas `metricas_retraso` y `predicciones_retraso`, volumen `modelos` y una ejecución en MLflow |
# MAGIC
# MAGIC **Cómputo:** Serverless con el entorno **Standard** (versión 6 si se puede elegir), que ya incluye scikit-learn, pandas, matplotlib, joblib y MLflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Datos y referencia
# MAGIC ### 6.1 · Comprobar la tabla Gold

# COMMAND ----------

display(spark.sql("""
    SELECT COUNT(*) AS registros, MIN(flight_date) AS primera_fecha, MAX(flight_date) AS ultima_fecha,
           COUNT(DISTINCT flight_id) AS vuelos_unicos,
           SUM(CASE WHEN arrival_delay IS NULL OR departure_delay IS NULL THEN 1 ELSE 0 END) AS incompletos
    FROM aerolinea_tf.gold.ml_flights
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 · Cargar las bibliotecas

# COMMAND ----------

import joblib
import matplotlib.pyplot as plt
import mlflow
import pandas as pd
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.tree import DecisionTreeRegressor

mlflow.autolog(disable=True)
print("scikit-learn:", sklearn.__version__)
print("MLflow:", mlflow.__version__)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 · Leer y validar los datos
# MAGIC Son unos 49.000 vuelos: caben en memoria y se convierten a pandas. Se ordenan por fecha e identificador para que el resultado sea reproducible.

# COMMAND ----------

columnas = ["flight_id", "flight_date", "airline_id", "origin_airport", "destination_airport",
            "distance_km", "day_of_week", "departure_delay", "arrival_delay"]
datos = (spark.table("aerolinea_tf.gold.ml_flights")
         .select(*columnas)
         .orderBy("flight_date", "flight_id")
         .toPandas())
datos["flight_date"] = pd.to_datetime(datos["flight_date"])
datos["distance_km"] = datos["distance_km"].astype(float)
for c in ["day_of_week", "departure_delay", "arrival_delay"]:
    datos[c] = datos[c].astype(int)

display(datos.head(10))
print("Filas:", len(datos))
print("Valores faltantes:", int(datos.isna().sum().sum()))
assert datos["flight_id"].is_unique and datos.isna().sum().sum() == 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 · Seleccionar variables y separar entrenamiento y evaluación
# MAGIC **Variables predictoras** (todas conocidas al despegar):
# MAGIC - Categóricas: `airline_id`, `origin_airport`, `destination_airport`.
# MAGIC - Numéricas: `distance_km`, `day_of_week` y `departure_delay`.
# MAGIC
# MAGIC **Fuera del modelo:** `flight_id` y `flight_date` (solo identifican y ordenan), y **el mes**: con un corte temporal, junio no aparece en entrenamiento, así que el árbol no podría aprender nada útil de él. `delay_total`, `on_time` y `delay_category` se calculan con el retraso de llegada: usarlos sería **fuga de información**.
# MAGIC
# MAGIC **Separación temporal**, fijada antes de entrenar: enero a mayo de 2025 para entrenar y **junio de 2025** para evaluar, como si el modelo se usara el mes siguiente.

# COMMAND ----------

corte = pd.Timestamp("2025-06-01")
entrenamiento = datos.loc[datos["flight_date"] < corte].copy()
evaluacion = datos.loc[datos["flight_date"] >= corte].copy()

categoricas = ["airline_id", "origin_airport", "destination_airport"]
numericas = ["distance_km", "day_of_week", "departure_delay"]
variables = categoricas + numericas
objetivo = "arrival_delay"

X_train, y_train = entrenamiento[variables], entrenamiento[objetivo]
X_eval, y_eval = evaluacion[variables], evaluacion[objetivo]

display(pd.DataFrame([
    {"conjunto": "entrenamiento", "filas": len(entrenamiento),
     "desde": entrenamiento["flight_date"].min().date(), "hasta": entrenamiento["flight_date"].max().date()},
    {"conjunto": "evaluacion", "filas": len(evaluacion),
     "desde": evaluacion["flight_date"].min().date(), "hasta": evaluacion["flight_date"].max().date()},
]))
assert not set(entrenamiento["flight_id"]) & set(evaluacion["flight_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.5 · Referencia del promedio
# MAGIC Aprende solo el retraso de llegada medio de entrenamiento y lo predice para todos los vuelos de junio.

# COMMAND ----------

referencia = DummyRegressor(strategy="mean")
referencia.fit(X_train, y_train)
pred_base = referencia.predict(X_eval)
print(f"Promedio aprendido: {y_train.mean():.2f} min")
print(f"MAE de referencia: {mean_absolute_error(y_eval, pred_base):.2f} min")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modelo, evaluación y seguimiento
# MAGIC ### 6.6 · Pipeline con árbol de regresión
# MAGIC `OneHotEncoder` aprende las categorías en entrenamiento (`handle_unknown="ignore"` acepta una categoría nueva sin fallar). El árbol se limita a profundidad 6 y 50 vuelos por hoja para controlar el sobreajuste.
# MAGIC
# MAGIC Además se entrena un **segundo árbol sin `departure_delay`**: responde a la pregunta "¿cuánto se puede anticipar **antes** de que el avión salga?", con solo la aerolínea, la ruta, la distancia y el día.

# COMMAND ----------

def pipeline_arbol(numericas_usadas):
    preparacion = ColumnTransformer([
        ("categorias", OneHotEncoder(handle_unknown="ignore"), categoricas),
        ("numericas", "passthrough", numericas_usadas),
    ])
    return Pipeline([
        ("preparacion", preparacion),
        ("arbol", DecisionTreeRegressor(max_depth=6, min_samples_leaf=50, random_state=42)),
    ])

modelo = pipeline_arbol(numericas)
modelo.fit(X_train, y_train)
pred_arbol = modelo.predict(X_eval)
pred_train = modelo.predict(X_train)

numericas_antes = ["distance_km", "day_of_week"]
modelo_antes = pipeline_arbol(numericas_antes)
modelo_antes.fit(X_train[categoricas + numericas_antes], y_train)
pred_antes = modelo_antes.predict(X_eval[categoricas + numericas_antes])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.7 · Comparar las métricas
# MAGIC Los tres modelos se evalúan sobre **los mismos vuelos de junio**. MAE y RMSE están en minutos; menor es mejor.

# COMMAND ----------

def fila(nombre, pred_tr, pred_ev):
    return {"modelo": nombre,
            "mae_entrenamiento_min": mean_absolute_error(y_train, pred_tr),
            "mae_evaluacion_min": mean_absolute_error(y_eval, pred_ev),
            "rmse_evaluacion_min": mean_squared_error(y_eval, pred_ev) ** 0.5}

metricas = pd.DataFrame([
    fila("promedio", referencia.predict(X_train), pred_base),
    fila("arbol_al_despegar", pred_train, pred_arbol),
    fila("arbol_antes_de_salir", modelo_antes.predict(X_train[categoricas + numericas_antes]), pred_antes),
])
display(metricas.round(2))

mae_base = metricas.loc[0, "mae_evaluacion_min"]
mae_arbol = metricas.loc[1, "mae_evaluacion_min"]
mejora = (1 - mae_arbol / mae_base) * 100
print(f"El árbol al despegar reduce el MAE de la referencia en un {mejora:.1f} %" if mae_arbol < mae_base
      else "El árbol no mejora el MAE de la referencia en esta evaluación.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.8 · Revisar las predicciones por vuelo
# MAGIC Se conservan los identificadores para relacionar cada predicción con su vuelo. Los errores más grandes muestran qué casos el modelo no anticipa.

# COMMAND ----------

resultados = evaluacion[["flight_id", "flight_date", "airline_id", "origin_airport",
                         "destination_airport", "departure_delay"]].copy()
resultados["arrival_delay_real"] = y_eval.to_numpy()
resultados["prediccion_promedio"] = pred_base.round(2)
resultados["prediccion_arbol"] = pred_arbol.round(2)
resultados["prediccion_arbol_antes_de_salir"] = pred_antes.round(2)
resultados["error_absoluto_arbol"] = (resultados["arrival_delay_real"] - pred_arbol).abs().round(2)
display(resultados.sort_values("error_absoluto_arbol", ascending=False).head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.9 · Visualizar las predicciones frente a los valores reales
# MAGIC La diagonal es la predicción perfecta. La referencia es una línea horizontal: predice lo mismo para todos los vuelos.

# COMMAND ----------

fig, ejes = plt.subplots(1, 2, figsize=(13, 5))
limite = (min(y_eval.min(), pred_arbol.min()) - 5, max(y_eval.max(), pred_arbol.max()) + 5)
ejes[0].scatter(y_eval, pred_antes, s=6, alpha=0.25, color="#9a9a9a", label="Árbol antes de salir")
ejes[0].scatter(y_eval, pred_arbol, s=6, alpha=0.35, color="#d26416", label="Árbol al despegar")
ejes[0].axhline(pred_base[0], color="#0072b9", linewidth=2, label="Promedio (referencia)")
ejes[0].plot(limite, limite, "--", color="black", linewidth=1, label="Predicción = real")
ejes[0].set(xlabel="Retraso de llegada real (min)", ylabel="Retraso de llegada estimado (min)",
            xlim=limite, ylim=limite, title="Junio 2025: real frente a estimado")
ejes[0].legend(fontsize=8, loc="upper left")

posiciones = range(len(metricas))
ejes[1].bar([p - 0.2 for p in posiciones], metricas["mae_evaluacion_min"], width=0.4, label="MAE", color="#0072b9")
ejes[1].bar([p + 0.2 for p in posiciones], metricas["rmse_evaluacion_min"], width=0.4, label="RMSE", color="#d26416")
ejes[1].set_xticks(list(posiciones))
ejes[1].set_xticklabels(metricas["modelo"])
ejes[1].set(ylabel="Error (min)", title="Error en evaluación: menor es mejor")
ejes[1].legend()
fig.suptitle(f"Retraso de llegada · entrenamiento ene–may · evaluación {len(evaluacion)} vuelos de junio")
fig.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.10 · Qué aprendió el árbol
# MAGIC Importancia de cada variable original (suma de sus columnas one-hot).

# COMMAND ----------

nombres = modelo.named_steps["preparacion"].get_feature_names_out()
importancias = pd.Series(modelo.named_steps["arbol"].feature_importances_, index=nombres)
importancia_variable = (importancias
                        .groupby(lambda n: next(v for v in variables if n.split("__", 1)[1].startswith(v)))
                        .sum().sort_values(ascending=False).round(4))
display(importancia_variable.rename("importancia").reset_index().rename(columns={"index": "variable"}))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.11 · Interpretación
# MAGIC Resultados de la ejecución (entrenamiento: 40.751 vuelos de enero a mayo de 2025; evaluación: 8.255 vuelos de junio):
# MAGIC
# MAGIC | Modelo | MAE entren. (min) | MAE eval. (min) | RMSE eval. (min) |
# MAGIC |---|---|---|---|
# MAGIC | Promedio (13,29 min para todos) | 18,26 | 18,31 | 23,00 |
# MAGIC | **Árbol al despegar** | 7,96 | **8,01** | **10,03** |
# MAGIC | Árbol antes de salir | 17,83 | 18,08 | 22,64 |
# MAGIC
# MAGIC - **El árbol al despegar mejora con claridad la referencia:** el MAE baja de 18,3 a 8,0 minutos (−56 %) y el RMSE de 23,0 a 10,0. La mejora viene casi toda del **retraso de salida** (importancia 0,999): el retraso con el que sale un vuelo se arrastra a la llegada (correlación de 0,90).
# MAGIC - **La aerolínea no añade información una vez conocido el retraso de salida.** JetBlue (B6) y Spirit (NK) llegan unos 10 minutos más tarde que las demás (apartado 2), pero también salen unos 10 minutos más tarde; en todas las aerolíneas la llegada es unos 5 minutos más tardía que la salida. Por eso el árbol no usa la aerolínea cuando tiene `departure_delay`.
# MAGIC - **No hay sobreajuste:** el MAE de entrenamiento (7,96) y el de evaluación (8,01) son casi iguales. Las hojas de al menos 50 vuelos agrupan casos parecidos sin memorizar el histórico.
# MAGIC - **Antes de la salida casi no se puede anticipar:** sin el retraso de salida, el árbol solo baja el MAE de 18,31 a 18,08 minutos. Distingue dos grupos (B6/NK ≈ 20 min y el resto ≈ 10 min, las dos franjas grises del gráfico), pero dentro de cada grupo el retraso varía mucho. La ruta, la distancia y el día de la semana no aportan, igual que en el apartado 3, donde las diferencias por mes, día y aeropuerto eran de menos de un minuto.
# MAGIC - **En el gráfico** los puntos del árbol al despegar siguen la diagonal, con una banda de unos ±10 minutos. Forman escalones horizontales porque cada hoja del árbol predice un único valor. La referencia es una línea plana que no distingue ningún vuelo.
# MAGIC - **Límites:** son seis meses de 2025 de una red de 10 aeropuertos y 6 aerolíneas, y la evaluación cubre un solo mes. No se observan el clima, la congestión ni las conexiones, que explican los errores más grandes (hasta 40 minutos). Antes de usar el modelo en operación habría que acordar qué error se acepta, evaluarlo en más periodos y vigilarlo con datos nuevos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.12 · Destinos de la práctica
# MAGIC Las salidas de ML van en un esquema propio, separado de Gold. El volumen `modelos` guarda el archivo del pipeline.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS aerolinea_tf.ml COMMENT 'Apartado 6: modelo, métricas y predicciones del retraso de llegada'")
spark.sql("CREATE VOLUME IF NOT EXISTS aerolinea_tf.ml.modelos")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.13 · Guardar y volver a cargar el modelo
# MAGIC Se guarda el pipeline completo (preparación y árbol) y se comprueba que el modelo recuperado da las mismas predicciones.

# COMMAND ----------

ruta_modelo = "/Volumes/aerolinea_tf/ml/modelos/arbol_retraso_llegada.joblib"
joblib.dump(modelo, ruta_modelo)
modelo_recuperado = joblib.load(ruta_modelo)

caso = X_eval.iloc[[0]].copy()
display(caso)
print(f"Estimación original:   {modelo.predict(caso)[0]:.2f} min")
print(f"Estimación recuperada: {modelo_recuperado.predict(caso)[0]:.2f} min")
assert (modelo_recuperado.predict(X_eval) == pred_arbol).all()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.14 · Registrar la ejecución en MLflow
# MAGIC En Databricks, MLflow crea el experimento asociado a este notebook. Se registran los parámetros, las métricas, el modelo, la figura y los datos de la evaluación.

# COMMAND ----------

with mlflow.start_run(run_name="arbol_retraso_llegada") as ejecucion:
    run_id = ejecucion.info.run_id
    mlflow.log_params({
        "tabla_fuente": "aerolinea_tf.gold.ml_flights",
        "objetivo": objetivo,
        "fecha_corte": str(corte.date()),
        "variables": ",".join(variables),
        "variables_antes_de_salir": ",".join(categoricas + numericas_antes),
        "max_depth": 6, "min_samples_leaf": 50, "random_state": 42,
        "filas_entrenamiento": len(entrenamiento),
        "filas_evaluacion": len(evaluacion),
        "scikit_learn_version": sklearn.__version__,
        "pandas_version": pd.__version__,
        "mlflow_version": mlflow.__version__,
        "joblib_version": joblib.__version__,
    })
    mlflow.log_metrics({
        "mae_promedio_min": float(mae_base),
        "mae_arbol_min": float(mae_arbol),
        "mae_arbol_entrenamiento_min": float(metricas.loc[1, "mae_entrenamiento_min"]),
        "rmse_promedio_min": float(metricas.loc[0, "rmse_evaluacion_min"]),
        "rmse_arbol_min": float(metricas.loc[1, "rmse_evaluacion_min"]),
        "mae_arbol_antes_de_salir_min": float(metricas.loc[2, "mae_evaluacion_min"]),
        "mejora_mae_pct": float(mejora),
    })
    mlflow.set_tags({"uso": "trabajo_final", "evaluacion": "corte_temporal_junio_2025"})
    mlflow.log_artifact(ruta_modelo, artifact_path="modelo")
    mlflow.log_figure(fig, "evaluacion_retraso_llegada.png")
    mlflow.log_text(metricas.to_csv(index=False), "metricas.csv")
    mlflow.log_text(resultados.to_csv(index=False), "predicciones_evaluacion.csv")
    mlflow.log_text(importancia_variable.to_csv(), "importancia_variables.csv")
    mlflow.log_dict({"variables": variables, "objetivo": objetivo, "fecha_corte": str(corte.date()),
                     "momento_prediccion": "al despegar (retraso de salida conocido)",
                     "filas_entrenamiento": len(entrenamiento), "filas_evaluacion": len(evaluacion)},
                    "definicion_evaluacion.json")
print("Run ID:", run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.15 · Publicar métricas y predicciones como tablas Delta
# MAGIC Ambas tablas llevan el `run_id`, así cada resultado se puede rastrear hasta su ejecución en MLflow. No se modifica ninguna tabla Gold.

# COMMAND ----------

metricas_tabla = metricas.round(4).copy()
metricas_tabla["run_id"] = run_id
metricas_tabla["fecha_corte"] = str(corte.date())
metricas_tabla["filas_entrenamiento"] = len(entrenamiento)
metricas_tabla["filas_evaluacion"] = len(evaluacion)
(spark.createDataFrame(metricas_tabla)
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable("aerolinea_tf.ml.metricas_retraso"))

salida = resultados.copy()
salida["flight_date"] = salida["flight_date"].dt.strftime("%Y-%m-%d")
salida["run_id"] = run_id
(spark.createDataFrame(salida)
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable("aerolinea_tf.ml.predicciones_retraso"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.16 · Comprobar la publicación
# MAGIC El promedio del error absoluto publicado debe coincidir con el MAE del árbol, y cada vuelo de junio debe tener una sola predicción.

# COMMAND ----------

display(spark.sql("""
    SELECT modelo, filas_entrenamiento, filas_evaluacion,
           ROUND(mae_evaluacion_min, 2) AS mae_min, ROUND(rmse_evaluacion_min, 2) AS rmse_min
    FROM aerolinea_tf.ml.metricas_retraso ORDER BY mae_evaluacion_min
"""))
comprobacion = spark.sql("""
    SELECT COUNT(*) AS predicciones, COUNT(DISTINCT flight_id) AS vuelos_unicos,
           ROUND(AVG(error_absoluto_arbol), 2) AS mae_arbol_min
    FROM aerolinea_tf.ml.predicciones_retraso
""").first()
display(spark.createDataFrame([comprobacion]))
assert comprobacion["predicciones"] == comprobacion["vuelos_unicos"] == len(evaluacion)
assert abs(comprobacion["mae_arbol_min"] - mae_arbol) < 0.01
