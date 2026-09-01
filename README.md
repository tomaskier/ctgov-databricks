# ctgov-databricks

A rebuild of [ctgov-etl](https://github.com/tomaskier/ctgov-etl)'s pipeline logic (extract from
ClinicalTrials.gov, bronze/silver layering, idempotent upserts, USDM overlay) on **PySpark +
Delta Lake + Databricks Workflows**, run on Databricks Community Edition (free tier). Built to
learn Spark/Databricks data-engineering patterns — the app layer (Streamlit UI, NL-to-SQL) from
the original project is intentionally out of scope here.

## Why the same pipeline twice

Reusing familiar business logic (same API, same fields, same idempotency requirements) keeps the
learning focused on the new tools — Spark DataFrames, Delta `MERGE`, Auto Loader, Databricks
Workflows — rather than relearning the domain from scratch.

## Architecture

```
ClinicalTrials.gov API v2
        |  extract.py (same pagination/watermark logic as the original, ported to Databricks)
        v
   Landing zone (DBFS)   raw JSON pages
        |  Auto Loader (cloudFiles, trigger(availableNow=True)) — the "streaming" layer
        v
   Bronze   Delta table, page-level (one row per landed file, not flattened)
        |  explode + typed projection, row_hash for change detection
        v
   Silver   Delta MERGE upsert — trials / sponsors / locations / interventions / outcomes / etl_runs
        |  re-read the same bronze pages, parsed into USDM-inspired entities
        v
   USDM     Delta tables — study / study_design / study_arm / objective / endpoint / ... / usdm_runs
```

Orchestrated as a 4-task Databricks Workflow: `extract → bronze (Auto Loader) → silver (merge) →
usdm overlay`.

## Key translations from the original (Postgres/pandas) pipeline

- **"Streaming"**: ClinicalTrials.gov has no push/webhook mechanism, so streaming here means
  Databricks Auto Loader's incremental file-discovery semantics (checkpoint-tracked, exactly-once
  file ingestion), run in triggered-batch mode inside a scheduled Workflow — not an always-on
  stream. This is the idiomatic Databricks pattern for a batch-cadence source landing files in
  cloud storage.
- **Idempotent upsert**: Postgres `INSERT...ON CONFLICT DO UPDATE WHERE row_hash IS DISTINCT FROM`
  becomes a Delta `MERGE INTO` with `whenMatchedUpdateAll(condition="NOT (t.row_hash <=> s.row_hash)")`
  — the null-safe `<=>` operator matters here, since plain `<>` isn't null-safe like Postgres's
  `IS DISTINCT FROM`.
- **Run tracking**: `find_unprocessed_runs()`'s Python set-difference becomes a `left_anti` join
  between bronze `(condition, run_id)` pairs and a Delta control table (`silver.etl_runs`).
- **No cross-table transactions in Delta**: the control-table row is written *last*, only after
  trials + child tables succeed, so a mid-batch failure is safely retryable (every write is
  independently idempotent).
- **Watermark state**: the original's local `extract_state.json` becomes a small Delta table
  (`control.watermarks`) — notebooks don't have reliable persistent local disk.
- **USDM surrogate keys**: Postgres `RETURNING id` auto-increment becomes deterministic
  hash-based keys (e.g. `sha2(concat_ws(':', nct_id, version_identifier), 256)`) — a deliberate
  improvement (idempotent reruns, no lookup round-trip), not just a workaround.

## Community Edition constraints (verified, not assumed)

_Filled in during Phase 0 — see the plan for what's being checked._

## Status

Phase 0 (workspace verification) in progress.
