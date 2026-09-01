# Databricks notebook source
import sys

sys.path.insert(0, "/Workspace/Users/tomas.kiersz@gmail.com/ctgov-databricks/src")

dbutils.widgets.text("condition", "", "Limit to one condition (optional)")
condition = dbutils.widgets.get("condition") or None

# COMMAND ----------
# spark.sql() runs one statement at a time - split the DDL file on ';' like the original's
# ensure_schema() does for Postgres, since Spark SQL has no multi-statement script execution.
ddl_text = open("/Workspace/Users/tomas.kiersz@gmail.com/ctgov-databricks/ddl/silver_ddl.sql").read()
code_lines = [line for line in ddl_text.splitlines() if not line.strip().startswith("--")]
for stmt in "\n".join(code_lines).split(";"):
    if stmt.strip():
        spark.sql(stmt)

# COMMAND ----------
from ctgov_databricks.load_silver import find_unprocessed_runs, load_batch

runs = find_unprocessed_runs(spark, condition)
print(f"{len(runs)} unprocessed run(s): {runs}")

for cond, run_id in runs:
    n = load_batch(spark, cond, run_id)
    print(f"loaded run={run_id} condition={cond} trials={n}")

# COMMAND ----------
display(spark.table("workspace.ctgov.silver_trials"))
