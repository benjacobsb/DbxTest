# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Validation Log Setup
# MAGIC
# MAGIC Creates the validation log schema and tables if they do not already exist.
# MAGIC Safe to run on every pipeline execution (idempotent).

# COMMAND ----------

catalog           = dbutils.widgets.get("catalog")
validation_schema = dbutils.widgets.get("validation_schema")

# COMMAND ----------

# MAGIC %md ## Create schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{validation_schema}")

# COMMAND ----------

# MAGIC %md ## validation_run — one row per notebook execution

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{validation_schema}.validation_run (
    run_id              STRING      NOT NULL,   -- UUID generated at orchestrator start
    pipeline_run_id     STRING,                 -- ADF / Databricks Workflows run ID
    batch_id            STRING,                 -- Batch/watermark being validated
    table_name          STRING,                 -- Target Silver table ('ALL' if full run)
    tier                TINYINT     NOT NULL,   -- 1=Completeness 2=SCD2 3=Business 4=Source
    tier_name           STRING      NOT NULL,   -- Human-readable tier label
    started_at          TIMESTAMP   NOT NULL,
    completed_at        TIMESTAMP,
    status              STRING      NOT NULL,   -- PASS | FAIL | WARN | RUNNING | ERROR
    total_checks        INT,
    passed_checks       INT,
    failed_checks       INT,
    warned_checks       INT,
    error_message       STRING,                 -- Top-level error if the notebook itself fails
    run_by              STRING,                 -- notebook path or job name
    env                 STRING                  -- dev / test / prod
)
USING DELTA
PARTITIONED BY (tier)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")

# COMMAND ----------

# MAGIC %md ## validation_check — one row per individual check

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{validation_schema}.validation_check (
    check_id            STRING      NOT NULL,   -- UUID
    run_id              STRING      NOT NULL,   -- FK → validation_run.run_id
    pipeline_run_id     STRING,
    batch_id            STRING,
    checked_at          TIMESTAMP   NOT NULL,
    tier                TINYINT     NOT NULL,
    tier_name           STRING      NOT NULL,
    table_name          STRING      NOT NULL,   -- Silver table being checked
    check_name          STRING      NOT NULL,   -- e.g. 'scd2_no_overlapping_dates'
    check_category      STRING      NOT NULL,   -- COMPLETENESS|SCD2|BUSINESS_RULE|SOURCE_RECON
    check_description   STRING,                 -- Plain-English description of the check
    status              STRING      NOT NULL,   -- PASS | FAIL | WARN
    severity            STRING      NOT NULL,   -- BLOCKING | WARNING | INFO
    expected_value      DOUBLE,                 -- Numeric expected (counts, sums, %)
    actual_value        DOUBLE,                 -- Numeric actual
    variance_value      DOUBLE,                 -- actual - expected
    variance_pct        DOUBLE,                 -- (actual - expected) / expected * 100
    failure_count       LONG,                   -- Number of offending rows
    failure_sample      STRING,                 -- JSON array of up to 10 bad row keys
    sql_query           STRING,                 -- The query that ran (for auditability)
    execution_ms        LONG                    -- How long the check took
)
USING DELTA
PARTITIONED BY (tier, checked_at)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.logRetentionDuration' = 'interval 90 days'
)
""")

# COMMAND ----------

# MAGIC %md ## validation_source_recon_snapshot — Tier 4 historical trend table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{validation_schema}.validation_source_recon_snapshot (
    snapshot_id         STRING      NOT NULL,
    run_id              STRING      NOT NULL,
    snapshot_date       DATE        NOT NULL,
    table_name          STRING      NOT NULL,
    metric_name         STRING      NOT NULL,   -- e.g. 'row_count' | 'sum_revenue'
    source_value        DOUBLE,
    silver_value        DOUBLE,
    variance_value      DOUBLE,
    variance_pct        DOUBLE,
    within_threshold    BOOLEAN,
    threshold_pct       DOUBLE
)
USING DELTA
PARTITIONED BY (snapshot_date)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

# COMMAND ----------

print("✅ Validation log infrastructure ready.")
dbutils.notebook.exit("PASS")
