import json
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp

from ctgov_databricks.config import LANDING_VOLUME
from ctgov_databricks.transform_silver import (
    deduped_studies,
    parse_interventions,
    parse_locations,
    parse_outcomes,
    parse_sponsors,
    parse_trials,
)

BRONZE_TABLE = "workspace.ctgov.bronze_pages"
ETL_RUNS_TABLE = "workspace.ctgov.silver_etl_runs"
TRIALS_TABLE = "workspace.ctgov.silver_trials"
SPONSORS_TABLE = "workspace.ctgov.silver_sponsors"
LOCATIONS_TABLE = "workspace.ctgov.silver_locations"
INTERVENTIONS_TABLE = "workspace.ctgov.silver_interventions"
OUTCOMES_TABLE = "workspace.ctgov.silver_outcomes"


def find_unprocessed_runs(spark, condition: str | None = None) -> list[tuple[str, str]]:
    bronze = spark.table(BRONZE_TABLE).select("condition", "run_id").distinct()
    if condition:
        bronze = bronze.filter(col("condition") == condition)
    processed = spark.table(ETL_RUNS_TABLE).select("condition", "run_id")
    unprocessed = bronze.join(processed, ["condition", "run_id"], "left_anti")
    return [(r["condition"], r["run_id"]) for r in unprocessed.orderBy("condition", "run_id").collect()]


def merge_trials(spark, trials_df: DataFrame) -> None:
    trials_df = trials_df.withColumn("loaded_at", current_timestamp())
    target = DeltaTable.forName(spark, TRIALS_TABLE)
    (
        target.alias("t")
        .merge(trials_df.alias("s"), "t.nct_id = s.nct_id")
        # <=> is null-safe equality, unlike plain <> - the Delta equivalent of Postgres's
        # IS DISTINCT FROM. Plain <> would treat NULL <> NULL as unknown, not true, and silently
        # skip updates on rows where row_hash is somehow null.
        .whenMatchedUpdateAll(condition="NOT (t.row_hash <=> s.row_hash)")
        .whenNotMatchedInsertAll()
        .execute()
    )


def replace_children(spark, table_name: str, df: DataFrame, nct_ids: list[str]) -> None:
    if nct_ids:
        DeltaTable.forName(spark, table_name).delete(col("nct_id").isin(nct_ids))
    if df.take(1):
        df.write.format("delta").mode("append").saveAsTable(table_name)


def upsert_etl_run(spark, manifest: dict, condition: str, run_id: str, trials_loaded: int) -> None:
    spark.sql(
        f"""
        MERGE INTO {ETL_RUNS_TABLE} AS t
        USING (
            SELECT :condition AS condition, :run_id AS run_id, :since AS since,
                   :full_refresh AS full_refresh, :status_filter AS status_filter,
                   :phase_filter AS phase_filter, :study_type_filter AS study_type_filter,
                   :page_count AS page_count, :total_count AS total_count,
                   :trials_loaded AS trials_loaded
        ) AS s
        ON t.condition = s.condition AND t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET
            since = s.since, full_refresh = s.full_refresh, status_filter = s.status_filter,
            phase_filter = s.phase_filter, study_type_filter = s.study_type_filter,
            page_count = s.page_count, total_count = s.total_count,
            trials_loaded = s.trials_loaded, silver_loaded_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (
            condition, run_id, since, full_refresh, status_filter, phase_filter,
            study_type_filter, page_count, total_count, trials_loaded, silver_loaded_at
        ) VALUES (
            s.condition, s.run_id, s.since, s.full_refresh, s.status_filter, s.phase_filter,
            s.study_type_filter, s.page_count, s.total_count, s.trials_loaded, current_timestamp()
        )
        """,
        args={
            "condition": condition,
            "run_id": run_id,
            "since": manifest.get("since"),
            "full_refresh": manifest.get("full_refresh", False),
            "status_filter": manifest.get("status"),
            "phase_filter": manifest.get("phase"),
            "study_type_filter": manifest.get("study_type"),
            "page_count": manifest.get("page_count"),
            "total_count": manifest.get("total_count"),
            "trials_loaded": trials_loaded,
        },
    )


def load_batch(spark, condition: str, run_id: str) -> int:
    bronze_batch = spark.table(BRONZE_TABLE).filter(
        (col("condition") == condition) & (col("run_id") == run_id)
    )
    deduped = deduped_studies(bronze_batch).cache()

    trials_df = parse_trials(deduped, condition, run_id)
    nct_ids = [r["nct_id"] for r in trials_df.select("nct_id").collect()]
    trials_loaded = len(nct_ids)

    # Control-table row written last, after trials + children succeed - Delta has no
    # cross-table transaction like the original's single Postgres `engine.begin()`, so this
    # ordering is what makes a mid-batch failure safely retryable: every write here is
    # independently idempotent, and a run only counts as "processed" once everything landed.
    merge_trials(spark, trials_df)
    replace_children(spark, SPONSORS_TABLE, parse_sponsors(deduped), nct_ids)
    replace_children(spark, LOCATIONS_TABLE, parse_locations(deduped), nct_ids)
    replace_children(spark, INTERVENTIONS_TABLE, parse_interventions(deduped), nct_ids)
    replace_children(spark, OUTCOMES_TABLE, parse_outcomes(deduped), nct_ids)

    manifest_path = Path(LANDING_VOLUME) / condition / run_id / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    upsert_etl_run(spark, manifest, condition, run_id, trials_loaded)

    deduped.unpersist()
    return trials_loaded
