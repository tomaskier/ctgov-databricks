# Databricks notebook source
import sys

sys.path.insert(0, "/Workspace/Users/tomas.kiersz@gmail.com/ctgov-databricks/src")

dbutils.widgets.text("conditions", "diabetes", "Comma-separated conditions")
dbutils.widgets.text("status", "", "overallStatus filter (optional)")
dbutils.widgets.text("phase", "", "Phase filter (optional)")
dbutils.widgets.text("study_type", "", "Study type filter (optional)")
dbutils.widgets.dropdown("full_refresh", "False", ["False", "True"], "Ignore watermark")

conditions = [c.strip() for c in dbutils.widgets.get("conditions").split(",") if c.strip()]
status = dbutils.widgets.get("status") or None
phase = dbutils.widgets.get("phase") or None
study_type = dbutils.widgets.get("study_type") or None
full_refresh = dbutils.widgets.get("full_refresh") == "True"

# COMMAND ----------
from ctgov_databricks.extract import run_extract

for condition in conditions:
    out_dir = run_extract(
        spark,
        condition,
        full_refresh=full_refresh,
        status=status,
        phase=phase,
        study_type=study_type,
    )
    print(f"{condition}: wrote {out_dir}")
