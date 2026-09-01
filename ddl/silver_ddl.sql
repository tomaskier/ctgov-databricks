-- PRIMARY KEY here is informational only - Delta doesn't enforce uniqueness. MERGE is what
-- actually guarantees one row per nct_id/condition+run_id; these are documentation, not a
-- constraint the engine checks.

CREATE TABLE IF NOT EXISTS workspace.ctgov.silver_etl_runs (
    condition STRING,
    run_id STRING,
    since STRING,
    full_refresh BOOLEAN NOT NULL,
    status_filter STRING,
    phase_filter STRING,
    study_type_filter STRING,
    page_count INT,
    total_count INT,
    trials_loaded INT,
    silver_loaded_at TIMESTAMP,
    CONSTRAINT silver_etl_runs_pk PRIMARY KEY (condition, run_id)
);

CREATE TABLE IF NOT EXISTS workspace.ctgov.silver_trials (
    nct_id STRING NOT NULL,
    brief_title STRING,
    official_title STRING,
    overall_status STRING,
    study_type STRING,
    phases ARRAY<STRING>,
    brief_summary STRING,
    conditions ARRAY<STRING>,
    keywords ARRAY<STRING>,
    start_date STRING,
    primary_completion_date STRING,
    completion_date STRING,
    enrollment_count INT,
    enrollment_type STRING,
    minimum_age STRING,
    maximum_age STRING,
    sex STRING,
    healthy_volunteers BOOLEAN,
    eligibility_criteria STRING,
    last_update_post_date STRING,
    study_first_post_date STRING,
    source_condition STRING NOT NULL,
    source_run_id STRING NOT NULL,
    row_hash STRING NOT NULL,
    loaded_at TIMESTAMP,
    CONSTRAINT silver_trials_pk PRIMARY KEY (nct_id)
);

-- Child tables: no surrogate id column. The original's bigserial `id` was never referenced as
-- an FK by anything else, and these are delete-by-nct_id + reinsert per batch (not merged), so
-- an auto-increment key would add complexity (Delta IDENTITY columns + streaming writes) for no
-- benefit - a deliberate simplification, not a fidelity gap that matters.

CREATE TABLE IF NOT EXISTS workspace.ctgov.silver_sponsors (
    nct_id STRING NOT NULL,
    sponsor_name STRING,
    sponsor_class STRING,
    is_lead BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS workspace.ctgov.silver_locations (
    nct_id STRING NOT NULL,
    facility STRING,
    city STRING,
    state STRING,
    country STRING,
    status STRING,
    latitude DOUBLE,
    longitude DOUBLE
);

CREATE TABLE IF NOT EXISTS workspace.ctgov.silver_interventions (
    nct_id STRING NOT NULL,
    intervention_type STRING,
    name STRING,
    description STRING
);

CREATE TABLE IF NOT EXISTS workspace.ctgov.silver_outcomes (
    nct_id STRING NOT NULL,
    outcome_type STRING NOT NULL,
    measure STRING,
    description STRING,
    time_frame STRING
);
