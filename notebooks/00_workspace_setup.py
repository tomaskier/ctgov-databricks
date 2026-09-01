# Databricks notebook source
# Phase 0 — sanity checks, verified against Databricks Free Edition (serverless, Unity Catalog
# on, public DBFS root disabled). See README.md for what each of these findings means.

# COMMAND ----------
print(spark.version)

# COMMAND ----------
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.ctgov")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.ctgov.landing")

# COMMAND ----------
dbutils.fs.mkdirs("/Volumes/workspace/ctgov/landing/_sanity")
dbutils.fs.put("/Volumes/workspace/ctgov/landing/_sanity/hello.txt", "hello from ctgov-databricks", overwrite=True)
print(dbutils.fs.head("/Volumes/workspace/ctgov/landing/_sanity/hello.txt"))

# COMMAND ----------
# %pip install -e <repo path> is unreliable on serverless compute (see README) - add src/ to
# sys.path directly instead. Replace <you> with your Databricks username.
import sys
sys.path.insert(0, "/Workspace/Users/<you>/ctgov-databricks/src")

from ctgov_databricks import config
print("import worked")
