# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Tier 1: Completeness Validation (Bronze → Silver)
# MAGIC
# MAGIC Checks that every Bronze record ingested in this batch is accounted for
# MAGIC in Silver — either as a new current record or a correctly closed historical row.
# MAGIC
# MAGIC **All failures here are BLOCKING.**

# COMMAND ----------

# MAGIC %run ./validation_helpers

# COMMAND ----------

catalog           = dbutils.widgets.get("catalog")
bronze_schema     = dbutils.widgets.get("bronze_schema")
silver_schema     = dbutils.widgets.get("silver_schema")
validation_schema = dbutils.widgets.get("validation_schema")
table_name        = dbutils.widgets.get("table_name")   # blank = all tables
pipeline_run_id   = dbutils.widgets.get("pipeline_run_id")
batch_id          = dbutils.widgets.get("batch_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table registry
# MAGIC
# MAGIC Edit this dict to reflect your actual Silver tables.
# MAGIC Keys:   Silver table name
# MAGIC Values: dict with
# MAGIC   - bronze_table   : corresponding Bronze table name
# MAGIC   - business_key   : list of columns that uniquely identify a business entity
# MAGIC   - batch_col      : column in Bronze that identifies the ingestion batch
# MAGIC   - numeric_cols   : columns to sum for aggregate reconciliation

# COMMAND ----------

TABLE_REGISTRY = {
    "customer": {
        "bronze_table":  "customer",
        "business_key":  ["customer_id"],
        "batch_col":     "_batch_id",
        "numeric_cols":  [],
    },
    "order": {
        "bronze_table":  "order",
        "business_key":  ["order_id"],
        "batch_col":     "_batch_id",
        "numeric_cols":  ["order_total", "tax_amount"],
    },
    "order_line": {
        "bronze_table":  "order_line",
        "business_key":  ["order_id", "line_number"],
        "batch_col":     "_batch_id",
        "numeric_cols":  ["line_total", "quantity"],
    },
    # Add your tables here ...
}

tables_to_check = (
    {table_name: TABLE_REGISTRY[table_name]}
    if table_name and table_name in TABLE_REGISTRY
    else TABLE_REGISTRY
)

# COMMAND ----------

# MAGIC %md ## Run checks

# COMMAND ----------

with ValidationRunner(
    spark             = spark,
    catalog           = catalog,
    validation_schema = validation_schema,
    tier              = 1,
    tier_name         = "Completeness",
    pipeline_run_id   = pipeline_run_id,
    batch_id          = batch_id,
    run_by            = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
) as vr:

    for tbl, cfg in tables_to_check.items():
        bronze_fqn = f"{catalog}.{bronze_schema}.{cfg['bronze_table']}"
        silver_fqn = f"{catalog}.{silver_schema}.{tbl}"
        bk         = cfg["business_key"]
        bk_csv     = ", ".join(bk)
        batch_col  = cfg["batch_col"]

        # ------------------------------------------------------------------
        # CHECK 1.1 — Row count: Bronze batch rows vs Silver rows touched
        # ------------------------------------------------------------------
        def check_row_counts():
            bronze_q = f"""
                SELECT COUNT(*) AS cnt
                FROM {bronze_fqn}
                WHERE {batch_col} = '{batch_id}'
            """
            silver_q = f"""
                SELECT COUNT(*) AS cnt
                FROM {silver_fqn}
                WHERE _batch_id = '{batch_id}'
            """
            b_cnt = spark.sql(bronze_q).collect()[0]["cnt"]
            s_cnt = spark.sql(silver_q).collect()[0]["cnt"]
            variance = s_cnt - b_cnt
            pct      = (variance / b_cnt * 100) if b_cnt > 0 else 0.0

            return {
                "status":          Status.PASS if variance == 0 else Status.FAIL,
                "expected_value":  float(b_cnt),
                "actual_value":    float(s_cnt),
                "variance_value":  float(variance),
                "variance_pct":    pct,
                "failure_count":   abs(variance) if variance != 0 else 0,
                "sql_query":       f"{bronze_q}\n---\n{silver_q}",
            }

        vr.run_check(
            check_name        = f"{tbl}.row_count_bronze_vs_silver",
            check_category    = CheckCategory.COMPLETENESS,
            table_name        = tbl,
            severity          = Severity.BLOCKING,
            check_description = (
                f"Row count in Bronze batch '{batch_id}' must match rows "
                f"written to Silver in the same batch."
            ),
            query_fn          = check_row_counts,
        )

        # ------------------------------------------------------------------
        # CHECK 1.2 — Key coverage: every Bronze business key exists in Silver
        # ------------------------------------------------------------------
        def check_key_coverage():
            missing_sql = f"""
                SELECT b.{bk_csv}
                FROM {bronze_fqn} b
                WHERE b.{batch_col} = '{batch_id}'
                  AND NOT EXISTS (
                      SELECT 1 FROM {silver_fqn} s
                      WHERE {' AND '.join(f's.{k} = b.{k}' for k in bk)}
                  )
            """
            missing_df  = spark.sql(missing_sql)
            missing_cnt = missing_df.count()
            sample      = collect_failure_sample(missing_df, bk) if missing_cnt > 0 else None

            return {
                "status":        Status.PASS if missing_cnt == 0 else Status.FAIL,
                "failure_count": missing_cnt,
                "failure_sample": sample,
                "sql_query":     missing_sql,
            }

        vr.run_check(
            check_name        = f"{tbl}.key_coverage_bronze_to_silver",
            check_category    = CheckCategory.COMPLETENESS,
            table_name        = tbl,
            severity          = Severity.BLOCKING,
            check_description = (
                "Every business key present in Bronze must exist in Silver "
                "(at least one record, current or historical)."
            ),
            query_fn          = check_key_coverage,
        )

        # ------------------------------------------------------------------
        # CHECK 1.3 — Numeric sum reconciliation (per configured columns)
        # ------------------------------------------------------------------
        for col in cfg.get("numeric_cols", []):
            def check_numeric_sum(col=col):
                bronze_sum_sql = f"""
                    SELECT COALESCE(SUM({col}), 0) AS s
                    FROM {bronze_fqn}
                    WHERE {batch_col} = '{batch_id}'
                """
                silver_sum_sql = f"""
                    SELECT COALESCE(SUM({col}), 0) AS s
                    FROM {silver_fqn}
                    WHERE _batch_id = '{batch_id}'
                      AND is_current = true
                """
                b_sum   = spark.sql(bronze_sum_sql).collect()[0]["s"]
                s_sum   = spark.sql(silver_sum_sql).collect()[0]["s"]
                variance = float(s_sum) - float(b_sum)
                pct      = (variance / float(b_sum) * 100) if b_sum != 0 else 0.0

                return {
                    "status":         Status.PASS if abs(pct) < 0.001 else Status.FAIL,
                    "expected_value": float(b_sum),
                    "actual_value":   float(s_sum),
                    "variance_value": variance,
                    "variance_pct":   pct,
                    "sql_query":      f"{bronze_sum_sql}\n---\n{silver_sum_sql}",
                }

            vr.run_check(
                check_name        = f"{tbl}.numeric_sum_{col}",
                check_category    = CheckCategory.COMPLETENESS,
                table_name        = tbl,
                severity          = Severity.BLOCKING,
                check_description = (
                    f"Sum of {col} across current Silver rows for this batch "
                    "must match the Bronze source sum."
                ),
                query_fn          = check_numeric_sum,
            )

    overall = vr.overall_status()

# COMMAND ----------

print(f"\nTier 1 overall: {overall.value}")
for r in vr.results:
    icon = "✅" if r.status == Status.PASS else "❌"
    print(f"  {icon}  {r.check_name}  ({r.status.value})")

dbutils.notebook.exit(overall.value)
