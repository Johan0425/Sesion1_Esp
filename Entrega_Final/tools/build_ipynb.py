"""Genera los .ipynb de la entrega a partir de los notebooks fuente de Databricks (.py).

Cada bloque separado por "# COMMAND ----------" pasa a ser una celda; los bloques
"# MAGIC %md" pasan a Markdown. Databricks importa el .ipynb con la misma estructura.

Uso (desde la carpeta de la entrega):
    python tools/build_ipynb.py
"""
import json
from pathlib import Path

ENTREGA = Path(__file__).resolve().parent.parent
ENCABEZADO = "# Databricks notebook source"
SEPARADOR = "# COMMAND ----------"


def celdas(texto):
    resultado = []
    for bloque in texto[len(ENCABEZADO):].split(SEPARADOR):
        lineas = bloque.strip("\n").splitlines()
        if not lineas:
            continue
        if all(l.startswith("# MAGIC") for l in lineas):
            contenido = [l[len("# MAGIC "):] if l.startswith("# MAGIC ") else "" for l in lineas]
            if contenido[0].startswith("%md"):
                contenido[0] = contenido[0][len("%md"):].lstrip()
                if not contenido[0]:
                    contenido = contenido[1:]
            resultado.append(("markdown", contenido))
        else:
            resultado.append(("code", lineas))
    return resultado


def a_ipynb(ruta_py):
    texto = ruta_py.read_text(encoding="utf-8")
    assert texto.startswith(ENCABEZADO), f"{ruta_py} no es un notebook fuente de Databricks"
    cells = []
    for tipo, lineas in celdas(texto):
        fuente = [l + "\n" for l in lineas[:-1]] + [lineas[-1]]
        celda = {"cell_type": tipo, "metadata": {}, "source": fuente}
        if tipo == "code":
            celda.update(execution_count=None, outputs=[])
        cells.append(celda)
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python"},
            "application/vnd.databricks.v1+notebook": {"language": "python", "notebookName": ruta_py.stem},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    destino = ruta_py.with_suffix(".ipynb")
    destino.write_text(json.dumps(notebook, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    print(f"{destino.relative_to(ENTREGA)}: {len(cells)} celdas")


if __name__ == "__main__":
    for ruta in sorted((ENTREGA / "notebooks").glob("*.py")):
        a_ipynb(ruta)
