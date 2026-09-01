# Databricks notebook source
# Auto Loader: incrementally discovers new JSON files landed by 01_extract_to_landing.py and
# appends them (page-level, not flattened - flattening is a Silver concern) into bronze_pages.
# trigger(availableNow=True) processes everything currently available then stops, rather than
# running as an always-on stream - the right shape for a Workflow task on a batch-cadence source.

from pyspark.sql.functions import col, regexp_extract

LANDING_PATH = "/Volumes/workspace/ctgov/landing"
SCHEMA_LOCATION = "/Volumes/workspace/ctgov/checkpoints/bronze_schema"
CHECKPOINT_PATH = "/Volumes/workspace/ctgov/checkpoints/bronze"
TARGET_TABLE = "workspace.ctgov.bronze_pages"

# COMMAND ----------
raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiLine", "true")  # each landed file is one JSON object, not JSON-lines
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("pathGlobFilter", "page_*.json")  # manifest.json has a different shape - exclude it
    .load(LANDING_PATH)
)

bronze = raw.withColumn(
    "condition", regexp_extract(col("_metadata.file_path"), r"/landing/([^/]+)/", 1)
).withColumn(
    "run_id", regexp_extract(col("_metadata.file_path"), r"/landing/[^/]+/([^/]+)/", 1)
)

# COMMAND ----------
query = (
    bronze.writeStream.format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)
query.awaitTermination()

# COMMAND ----------
display(spark.table(TARGET_TABLE).select("condition", "run_id", "totalCount", "nextPageToken"))
