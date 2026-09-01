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
   Landing zone (UC Volume: workspace.ctgov.landing)   raw JSON pages
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

## Databricks Free Edition — what's actually true here (verified in Phase 0)

This workspace is **Databricks Free Edition**, the newer serverless-based replacement for the old
"Community Edition" — several assumptions from earlier CE docs don't hold here:

- **Compute is serverless by default** — `Default Interactive Compute` (notebooks) and
  `Default Automated Compute` (Jobs) come pre-provisioned. No manual cluster sizing, and no
  auto-termination-kills-a-scheduled-job concern the way classic clusters had.
- **Unity Catalog is on**, not the legacy Hive metastore — three catalogs exist out of the box
  (`workspace`, `system`, `samples`). We use `workspace.ctgov` as our schema.
- **The public DBFS root is disabled** (`DBFS_DISABLED` on any `dbfs:/...` write) — governance
  default on UC-enabled workspaces. File storage uses a **Unity Catalog Volume** instead:
  `workspace.ctgov.landing`, created via `CREATE VOLUME`, written/read through the same
  `dbutils.fs.*` calls but at `/Volumes/workspace/ctgov/landing/...` paths.
- **Databricks Repos is now "Git folders"** in the UI, reached via Workspace → Create/right-click
  → "Git folder", not a dedicated "Repos" sidebar entry.
- **`%pip install -e .` reports success but isn't reliably importable on serverless** — even
  before a kernel restart. Rather than fight it, notebooks add `src/` to `sys.path` directly:
  ```python
  import sys
  sys.path.insert(0, "/Workspace/Users/<you>/ctgov-databricks/src")
  from ctgov_databricks import config
  ```

## Status

Phase 0 (workspace verification) complete. Phase 1 (repo scaffold) complete. Phase 2 (extract to
landing) complete — verified end-to-end against the real API. Phase 3 (bronze via Auto Loader) in
progress.
