"""Genera el archivo .dbc importable en Databricks a partir del notebook fuente .py.

Uso:
    python3 tools/build_dbc.py [01_Fundamentos_Databricks.py] [01_Fundamentos_Databricks.dbc]
"""
import json
import sys
import uuid
import zipfile
from pathlib import Path

ENCABEZADO = "# Databricks notebook source"
SEPARADOR = "# COMMAND ----------"


def leer_celdas(ruta_py):
    """Devuelve la lista de celdas (texto tal como se ve en Databricks) del notebook .py."""
    texto = Path(ruta_py).read_text(encoding="utf-8")
    if not texto.startswith(ENCABEZADO):
        raise ValueError(f"{ruta_py} no es un notebook fuente de Databricks")
    texto = texto[len(ENCABEZADO):]

    celdas = []
    for bloque in texto.split(SEPARADOR):
        lineas = bloque.strip("\n").splitlines()
        if not lineas:
            continue
        if all(l.startswith("# MAGIC") for l in lineas):
            lineas = [l[len("# MAGIC "):] if l.startswith("# MAGIC ") else "" for l in lineas]
        celdas.append("\n".join(lineas))
    return celdas


def leer_celdas_dbc(ruta_dbc):
    """Devuelve las celdas de un .dbc ordenadas por posición."""
    with zipfile.ZipFile(ruta_dbc) as z:
        nombre = next(n for n in z.namelist() if n.endswith(".python"))
        notebook = json.loads(z.read(nombre))
    comandos = sorted(notebook["commands"], key=lambda c: c["position"])
    return [c["command"] for c in comandos]


def _id():
    return uuid.uuid4().int % 10**16


def construir_dbc(ruta_py, ruta_dbc):
    nombre = Path(ruta_py).stem
    comandos = [
        {
            "version": "CommandV1",
            "origId": _id(),
            "guid": str(uuid.uuid4()),
            "subtype": "command",
            "commandType": "auto",
            "position": float(i + 1),
            "command": celda,
            "commandVersion": 0,
            "state": "input",
            "results": None,
            "errorSummary": None,
            "error": None,
            "workflows": [],
            "startTime": 0,
            "submitTime": 0,
            "finishTime": 0,
            "collapsed": False,
            "bindings": {},
            "inputWidgets": {},
            "displayType": "table",
            "width": "auto",
            "height": "auto",
            "xColumns": None,
            "yColumns": None,
            "pivotColumns": None,
            "pivotAggregation": None,
            "useConsistentColors": False,
            "customPlotOptions": {},
            "commentThread": [],
            "commentsVisible": False,
            "parentHierarchy": [],
            "diffInserts": [],
            "diffDeletes": [],
            "globalVars": {},
            "latestUser": "",
            "latestUserId": None,
            "commandTitle": "",
            "showCommandTitle": False,
            "hideCommandCode": False,
            "hideCommandResult": False,
            "iPythonMetadata": None,
            "streamStates": {},
            "nuid": str(uuid.uuid4()),
        }
        for i, celda in enumerate(leer_celdas(ruta_py))
    ]
    notebook = {
        "version": "NotebookV1",
        "origId": _id(),
        "name": nombre,
        "language": "python",
        "commands": comandos,
        "dashboards": [],
        "guid": str(uuid.uuid4()),
        "globalVars": {},
        "iPythonMetadata": None,
        "inputWidgets": {},
        "notebookMetadata": {"pythonIndentUnit": 4},
    }
    with zipfile.ZipFile(ruta_dbc, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"{nombre}.python", json.dumps(notebook, ensure_ascii=False))

    assert leer_celdas_dbc(ruta_dbc) == leer_celdas(ruta_py), "El .dbc no coincide con el .py"
    return len(comandos)


if __name__ == "__main__":
    raiz = Path(__file__).resolve().parent.parent
    origen = Path(sys.argv[1]) if len(sys.argv) > 1 else raiz / "01_Fundamentos_Databricks.py"
    destino = Path(sys.argv[2]) if len(sys.argv) > 2 else origen.with_suffix(".dbc")
    print(f"{destino.name}: {construir_dbc(origen, destino)} celdas")
