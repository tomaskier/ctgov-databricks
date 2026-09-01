# Databricks notebook source
# Phase 0 — sanity checks. Confirms DBFS write access, Spark/DBR version, and (once this repo is
# synced via Databricks Repos) that `src/ctgov_databricks` is importable.

# COMMAND ----------
print(spark.version)

# COMMAND ----------
dbutils.fs.mkdirs("dbfs:/ctgov/_sanity")
dbutils.fs.put("dbfs:/ctgov/_sanity/hello.txt", "hello from ctgov-databricks", overwrite=True)
print(dbutils.fs.head("dbfs:/ctgov/_sanity/hello.txt"))

# COMMAND ----------
dbutils.fs.ls("dbfs:/ctgov/_sanity")
