# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Tier 2: SCD2 Integrity Validation
# MAGIC
# MAGIC Validates the structural correctness of Slowly Changing Dimension Type 2
# MAGIC records in the Silver layer.  All failures are BLOCKING.
# MAGIC
# MAGIC Checks performed (for every configured table):
# MAGIC   2.1  No duplicate current records per business key
# MAGIC   2.2  No overlapping effective date ranges per business key
# MAGIC   2.3  effective_end_date of a closed record = effective_start_date of next version
# MAGIC   2.4  No NULL effective_start_date on any record
# MAGIC   2.5  No NULL effective_end_date on non-current (closed) records
# MAGIC   2.6  effective_start_date <= effective_end_date on all rows
# MAGIC   2.7  At least one current record exists for every known business key

# COMMAND ----------

# MAGIC %run ./validation_helpers

# COMMAND ----------

catalog           = dbutils.widgets.get("catalog")
silver_schema     = dbutils.widgets.get("silver_schema")
validation_schema = dbutils.widgets.get("validation_schema")
table_name        = dbutils.widgets.get("table_name")
pipeline_run_id   = dbutils.widgets.get("pipeline_run_id")
batch_id          = dbutils.widgets.get("batch_id")

# COMMAND ----------

# MAGIC %md ## Table registry — SCD2 columns

# COMMAND ----------

# Adjust these column names to match your Silver table conventions.
SCD2_DEFAULTS = {
    "is_current_col":        "is_current",
    "effective_start_col":   "effective_start_date",
    "effective_end_col":     "effective_end_date",
    "surrogate_key_col":     "silver_key",
}

TABLE_REGISTRY = {
    "customer": {
        "business_key": ["customer_id"],
        **SCD2_DEFAULTS,
    },
    "order": {
        "business_key": ["order_id"],
        **SCD2_DEFAULTS,
    },
    "order_line": {
        "business_key": ["order_id", "line_number"],
        **SCD2_DEFAULTS,
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
    tier              = 2,
    tier_name         = "SCD2 Integrity",
    pipeline_run_id   = pipeline_run_id,
    batch_id          = batch_id,
    run_by            = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
) as vr:

    for tbl, cfg in tables_to_check.items():
        silver_fqn   = f"{catalog}.{silver_schema}.{tbl}"
        bk           = cfg["business_key"]
        bk_csv       = ", ".join(bk)
        is_cur       = cfg["is_current_col"]
        eff_start    = cfg["effective_start_col"]
        eff_end      = cfg["effective_end_col"]

        # ------------------------------------------------------------------
        # CHECK 2.1 — No more than one current record per business key
        # ------------------------------------------------------------------
        def check_duplicate_current():
            sql = f"""
                SELECT {bk_csv}, COUNT(*) AS cnt
                FROM {silver_fqn}
                WHERE {is_cur} = true
                GROUP BY {bk_csv}
                HAVING COUNT(*) > 1
            """
            df  = spark.sql(sql)
            cnt = df.count()
            return {
                "status":         Status.PASS if cnt == 0 else Status.FAIL,
                "failure_count":  cnt,
                "failure_sample": collect_failure_sample(df, bk) if cnt > 0 else None,
                "sql_query":      sql,
            }

        vr.run_check(
            check_name        = f"{tbl}.scd2_no_duplicate_current",
            check_category    = CheckCategory.SCD2,
            table_name        = tbl,
            severity          = Severity.BLOCKING,
            check_description = "Each business key must have at most one is_current=true record.",
            query_fn          = check_duplicate_current,
        )

        # ------------------------------------------------------------------
        # CHECK 2.2 — No overlapping effective date ranges
        # ------------------------------------------------------------------
        def check_overlapping_dates():
            sql = f"""
                SELECT a.{bk_csv.replace(', ', ', a.')},
                       a.{eff_start} AS a_start, a.{eff_end} AS a_end,
                       b.{eff_start} AS b_start, b.{eff_end} AS b_end
                FROM {silver_fqn} a
                JOIN {silver_fqn} b
                  ON {' AND '.join(f'a.{k} = b.{k}' for k in bk)}
                 AND a.{eff_start} <  b.{eff_end}
                 AND a.{eff_end}   >  b.{eff_start}
                 AND a.{eff_start} <> b.{eff_start}
            """
            df  = spark.sql(sql)
            cnt = df.count()
            return {
                "status":         Status.PASS if cnt == 0 else Status.FAIL,
                "failure_count":  cnt,
                "failure_sample": collect_failure_sample(df, bk) if cnt > 0 else None,
                "sql_query":      sql,
            }

        vr.run_check(
            check_name        = f"{tbl}.scd2_no_overlapping_dates",
            check_category    = CheckCategory.SCD2,
            table_name        = tbl,
            severity          = Severity.BLOCKING,
            check_description = "No two records for the same business key may have overlapping effective date ranges.",
            query_fn          = check_overlapping_dates,
        )

        # ------------------------------------------------------------------
        # CHECK 2.3 — Closed record end_date = next version start_date
        # ------------------------------------------------------------------
        def check_chain_continuity():
            sql = f"""
                WITH ranked AS (
                    SELECT *,
                           LEAD({eff_start}) OVER (
                               PARTITION BY {bk_csv}
                               ORDER BY {eff_start}
                           ) AS next_start
                    FROM {silver_fqn}
                )
                SELECT {bk_csv}, {eff_start}, {eff_end}, next_start
                FROM ranked
                WHERE {is_cur} = false
                  AND next_start IS NOT NULL
                  AND {eff_end} <> next_start
            """
            df  = spark.sql(sql)
            cnt = df.count()
            return {
                "status":         Status.PASS if cnt == 0 else Status.FAIL,
                "failure_count":  cnt,
                "failure_sample": collect_failure_sample(df, bk) if cnt > 0 else None,
                "sql_query":      sql,
            }

        vr.run_check(
            check_name        = f"{tbl}.scd2_chain_continuity",
            check_category    = CheckCategory.SCD2,
            table_name        = tbl,
            severity          = Severity.BLOCKING,
            check_description = (
                "A closed record's effective_end_date must equal the "
                "effective_start_date of the next version for the same key."
            ),
            query_fn          = check_chain_continuity,
        )

        # ------------------------------------------------------------------
        # CHECK 2.4 — No NULL effective_start_date
        # ------------------------------------------------------------------
        def check_null_start():
            sql = f"""
                SELECT {bk_csv}
                FROM {silver_fqn}
                WHERE {eff_start} IS NULL
            """
            df  = spark.sql(sql)
            cnt = df.count()
            return {
                "status":         Status.PASS if cnt == 0 else Status.FAIL,
                "failure_count":  cnt,
                "failure_sample": collect_failure_sample(df, bk) if cnt > 0 else None,
                "sql_query":      sql,
            }

        vr.run_check(
            check_name        = f"{tbl}.scd2_no_null_effective_start",
            check_category    = CheckCategory.SCD2,
            table_name        = tbl,
            severity          = Severity.BLOCKING,
            check_description = "effective_start_date must never be NULL on any record.",
            query_fn          = check_null_start,
        )

        # ------------------------------------------------------------------
        # CHECK 2.5 — No NULL effective_end_date on closed (non-current) rows
        # ------------------------------------------------------------------
        def check_null_end():
            sql = f"""
                SELECT {bk_csv}
                FROM {silver_fqn}
                WHERE {is_cur} = false
                  AND {eff_end} IS NULL
            """
            df  = spark.sql(sql)
            cnt = df.count()
            return {
                "status":         Status.PASS if cnt == 0 else Status.FAIL,
                "failure_count":  cnt,
                "failure_sample": collect_failure_sample(df, bk) if cnt > 0 else None,
                "sql_query":      sql,
            }

        vr.run_check(
            check_name        = f"{tbl}.scd2_no_null_end_on_closed",
            check_category    = CheckCategory.SCD2,
            table_name        = tbl,
            severity          = Severity.BLOCKING,
            check_description = "Closed (is_current=false) records must have a non-NULL effective_end_date.",
            query_fn          = check_null_end,
        )

        # ------------------------------------------------------------------
        # CHECK 2.6 — start <= end on every row
        # ------------------------------------------------------------------
        def check_date_order():
            sql = f"""
                SELECT {bk_csv}, {eff_start}, {eff_end}
                FROM {silver_fqn}
                WHERE {eff_end} IS NOT NULL
                  AND {eff_start} > {eff_end}
            """
            df  = spark.sql(sql)
            cnt = df.count()
            return {
                "status":         Status.PASS if cnt == 0 else Status.FAIL,
                "failure_count":  cnt,
                "failure_sample": collect_failure_sample(df, bk) if cnt > 0 else None,
                "sql_query":      sql,
            }

        vr.run_check(
            check_name        = f"{tbl}.scd2_start_lte_end",
            check_category    = CheckCategory.SCD2,
            table_name        = tbl,
            severity          = Severity.BLOCKING,
            check_description = "effective_start_date must be <= effective_end_date on every row.",
            query_fn          = check_date_order,
        )

        # ------------------------------------------------------------------
        # CHECK 2.7 — Every business key has exactly one current record
        # ------------------------------------------------------------------
        def check_orphan_keys():
            sql = f"""
                SELECT {bk_csv}
                FROM {silver_fqn}
                GROUP BY {bk_csv}
                HAVING SUM(CASE WHEN {is_cur} = true THEN 1 ELSE 0 END) = 0
            """
            df  = spark.sql(sql)
            cnt = df.count()
            return {
                "status":         Status.PASS if cnt == 0 else Status.FAIL,
                "failure_count":  cnt,
                "failure_sample": collect_failure_sample(df, bk) if cnt > 0 else None,
                "sql_query":      sql,
            }

        vr.run_check(
            check_name        = f"{tbl}.scd2_no_orphan_keys",
            check_category    = CheckCategory.SCD2,
            table_name        = tbl,
            severity          = Severity.BLOCKING,
            check_description = "Every business key present in Silver must have at least one is_current=true record.",
            query_fn          = check_orphan_keys,
        )

    overall = vr.overall_status()

# COMMAND ----------

print(f"\nTier 2 overall: {overall.value}")
for r in vr.results:
    icon = "✅" if r.status == Status.PASS else "❌"
    print(f"  {icon}  {r.check_name}  ({r.status.value})")

dbutils.notebook.exit(overall.value)
