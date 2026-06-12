# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Validation Dashboard Queries
# MAGIC
# MAGIC Reference queries for monitoring and troubleshooting.
# MAGIC Attach this notebook to a Databricks SQL Dashboard or run ad-hoc.

# COMMAND ----------

# Set your catalog and schema here
CATALOG = "main"
SCHEMA  = "validation"
C = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Recent run summary — last 7 days

# COMMAND ----------

spark.sql(f"""
    SELECT
        DATE(started_at)      AS run_date,
        tier_name,
        status,
        total_checks,
        passed_checks,
        failed_checks,
        warned_checks,
        pipeline_run_id,
        batch_id,
        ROUND(DATEDIFF(SECOND, started_at, completed_at), 0) AS duration_sec
    FROM {C}.validation_run
    WHERE started_at >= CURRENT_DATE - INTERVAL 7 DAYS
    ORDER BY started_at DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. All FAIL and WARN checks — last 7 days

# COMMAND ----------

spark.sql(f"""
    SELECT
        checked_at,
        tier_name,
        table_name,
        check_name,
        status,
        severity,
        check_description,
        failure_count,
        expected_value,
        actual_value,
        ROUND(variance_pct, 2) AS variance_pct,
        failure_sample,
        execution_ms
    FROM {C}.validation_check
    WHERE status IN ('FAIL', 'WARN')
      AND checked_at >= CURRENT_DATE - INTERVAL 7 DAYS
    ORDER BY checked_at DESC, severity DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Check pass rate trend — by tier, by day

# COMMAND ----------

spark.sql(f"""
    SELECT
        DATE(checked_at)  AS check_date,
        tier_name,
        COUNT(*)          AS total,
        SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed,
        SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed,
        SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) AS warned,
        ROUND(
            SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
            1
        ) AS pass_rate_pct
    FROM {C}.validation_check
    WHERE checked_at >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY DATE(checked_at), tier_name
    ORDER BY check_date DESC, tier_name
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Source reconciliation drift over time

# COMMAND ----------

spark.sql(f"""
    SELECT
        snapshot_date,
        table_name,
        metric_name,
        source_value,
        silver_value,
        ROUND(variance_pct, 4)  AS variance_pct,
        within_threshold,
        threshold_pct
    FROM {C}.validation_source_recon_snapshot
    ORDER BY snapshot_date DESC, table_name, metric_name
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Slowest checks — performance monitoring

# COMMAND ----------

spark.sql(f"""
    SELECT
        check_name,
        table_name,
        tier_name,
        ROUND(AVG(execution_ms), 0)  AS avg_ms,
        ROUND(MAX(execution_ms), 0)  AS max_ms,
        COUNT(*)                     AS run_count
    FROM {C}.validation_check
    WHERE checked_at >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY check_name, table_name, tier_name
    ORDER BY avg_ms DESC
    LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Failure sample inspection — drill into a specific check

# COMMAND ----------

# Change these to investigate a specific failure
TARGET_CHECK = "order.scd2_no_duplicate_current"
TARGET_DATE  = "2025-01-01"   # or use CURRENT_DATE

spark.sql(f"""
    SELECT
        checked_at,
        batch_id,
        status,
        failure_count,
        failure_sample,
        sql_query
    FROM {C}.validation_check
    WHERE check_name  = '{TARGET_CHECK}'
      AND DATE(checked_at) >= '{TARGET_DATE}'
    ORDER BY checked_at DESC
    LIMIT 10
""").display()
