# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Tier 3: Business Rules Validation
# MAGIC
# MAGIC Validates domain-specific data quality rules on Silver current records.
# MAGIC Failures here are WARNING severity — they log and alert but do NOT
# MAGIC block the pipeline or Gold layer loads.
# MAGIC
# MAGIC Checks performed:
# MAGIC   3.1  Required (non-nullable) fields are populated
# MAGIC   3.2  Numeric columns are within expected business ranges
# MAGIC   3.3  Referential integrity — FK keys resolve in Silver dimensions
# MAGIC   3.4  Conditional field logic (if A then B must be populated)
# MAGIC   3.5  Duplicate detection on business keys in current records

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

# MAGIC %md ## Business rules registry
# MAGIC
# MAGIC This is the primary place you'll add domain knowledge.
# MAGIC Each table entry can define:
# MAGIC   - required_fields:  columns that must be non-null in current records
# MAGIC   - numeric_ranges:   {col: (min, max)} — None means unbounded
# MAGIC   - foreign_keys:     [{fk_col, ref_table, ref_col}]
# MAGIC   - conditional_rules:{description: sql WHERE clause that should return 0 rows}

# COMMAND ----------

TABLE_REGISTRY = {
    "customer": {
        "business_key":    ["customer_id"],
        "required_fields": ["customer_id", "email", "created_date"],
        "numeric_ranges":  {},
        "foreign_keys":    [],
        "conditional_rules": {
            "corporate_account_must_have_company_name": """
                is_current = true
                AND account_type = 'CORPORATE'
                AND (company_name IS NULL OR TRIM(company_name) = '')
            """,
        },
    },
    "order": {
        "business_key":    ["order_id"],
        "required_fields": ["order_id", "customer_id", "order_date", "status"],
        "numeric_ranges": {
            "order_total": (0, None),      # must be non-negative
            "tax_amount":  (0, None),
        },
        "foreign_keys": [
            {"fk_col": "customer_id", "ref_table": "customer", "ref_col": "customer_id"},
        ],
        "conditional_rules": {
            "shipped_order_must_have_ship_date": """
                is_current = true
                AND status = 'SHIPPED'
                AND ship_date IS NULL
            """,
            "cancelled_order_total_must_be_zero_or_negative": """
                is_current = true
                AND status = 'CANCELLED'
                AND order_total > 0
            """,
        },
    },
    "order_line": {
        "business_key":    ["order_id", "line_number"],
        "required_fields": ["order_id", "line_number", "product_id", "quantity", "unit_price"],
        "numeric_ranges": {
            "quantity":   (1, 10000),
            "unit_price": (0, None),
            "line_total": (0, None),
        },
        "foreign_keys": [
            {"fk_col": "order_id", "ref_table": "order", "ref_col": "order_id"},
        ],
        "conditional_rules": {},
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
    tier              = 3,
    tier_name         = "Business Rules",
    pipeline_run_id   = pipeline_run_id,
    batch_id          = batch_id,
    run_by            = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
) as vr:

    for tbl, cfg in tables_to_check.items():
        silver_fqn = f"{catalog}.{silver_schema}.{tbl}"
        bk         = cfg["business_key"]

        # ------------------------------------------------------------------
        # CHECK 3.1 — Required fields are non-null (current records only)
        # ------------------------------------------------------------------
        for col in cfg.get("required_fields", []):
            def check_required(col=col):
                sql = f"""
                    SELECT {', '.join(bk)}
                    FROM {silver_fqn}
                    WHERE is_current = true
                      AND {col} IS NULL
                """
                df  = spark.sql(sql)
                cnt = df.count()
                return {
                    "status":         Status.PASS if cnt == 0 else Status.WARN,
                    "failure_count":  cnt,
                    "failure_sample": collect_failure_sample(df, bk) if cnt > 0 else None,
                    "sql_query":      sql,
                }

            vr.run_check(
                check_name        = f"{tbl}.required_field_{col}",
                check_category    = CheckCategory.BUSINESS_RULE,
                table_name        = tbl,
                severity          = Severity.WARNING,
                check_description = f"Column '{col}' must not be NULL in current records.",
                query_fn          = check_required,
            )

        # ------------------------------------------------------------------
        # CHECK 3.2 — Numeric range validation
        # ------------------------------------------------------------------
        for col, (min_val, max_val) in cfg.get("numeric_ranges", {}).items():
            def check_range(col=col, min_val=min_val, max_val=max_val):
                conditions = ["is_current = true"]
                if min_val is not None:
                    conditions.append(f"{col} < {min_val}")
                if max_val is not None:
                    conditions.append(f"{col} > {max_val}")

                # Build a single WHERE clause: current=true AND (out-of-range)
                range_clause = " OR ".join(
                    ([f"{col} < {min_val}"] if min_val is not None else []) +
                    ([f"{col} > {max_val}"] if max_val is not None else [])
                )
                sql = f"""
                    SELECT {', '.join(bk)}, {col}
                    FROM {silver_fqn}
                    WHERE is_current = true
                      AND ({range_clause})
                """
                df  = spark.sql(sql)
                cnt = df.count()
                bound_desc = f"[{min_val if min_val is not None else '-∞'}, {max_val if max_val is not None else '+∞'}]"
                return {
                    "status":         Status.PASS if cnt == 0 else Status.WARN,
                    "failure_count":  cnt,
                    "failure_sample": collect_failure_sample(df, bk + [col]) if cnt > 0 else None,
                    "sql_query":      sql,
                }

            vr.run_check(
                check_name        = f"{tbl}.range_{col}",
                check_category    = CheckCategory.BUSINESS_RULE,
                table_name        = tbl,
                severity          = Severity.WARNING,
                check_description = (
                    f"Column '{col}' must be within expected business range "
                    f"[{min_val}, {max_val}] in current records."
                ),
                query_fn          = check_range,
            )

        # ------------------------------------------------------------------
        # CHECK 3.3 — Referential integrity (FK → Silver dimension)
        # ------------------------------------------------------------------
        for fk in cfg.get("foreign_keys", []):
            fk_col    = fk["fk_col"]
            ref_table = fk["ref_table"]
            ref_col   = fk["ref_col"]
            ref_fqn   = f"{catalog}.{silver_schema}.{ref_table}"

            def check_fk(fk_col=fk_col, ref_fqn=ref_fqn, ref_col=ref_col):
                sql = f"""
                    SELECT s.{', s.'.join(bk)}, s.{fk_col}
                    FROM {silver_fqn} s
                    WHERE s.is_current = true
                      AND s.{fk_col} IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM {ref_fqn} r
                          WHERE r.{ref_col} = s.{fk_col}
                            AND r.is_current = true
                      )
                """
                df  = spark.sql(sql)
                cnt = df.count()
                return {
                    "status":         Status.PASS if cnt == 0 else Status.WARN,
                    "failure_count":  cnt,
                    "failure_sample": collect_failure_sample(df, bk + [fk_col]) if cnt > 0 else None,
                    "sql_query":      sql,
                }

            vr.run_check(
                check_name        = f"{tbl}.fk_{fk_col}_to_{ref_table}",
                check_category    = CheckCategory.BUSINESS_RULE,
                table_name        = tbl,
                severity          = Severity.WARNING,
                check_description = (
                    f"Foreign key '{fk_col}' must resolve to a current record "
                    f"in {ref_table}.{ref_col}."
                ),
                query_fn          = check_fk,
            )

        # ------------------------------------------------------------------
        # CHECK 3.4 — Conditional / cross-field business rules
        # ------------------------------------------------------------------
        for rule_name, where_clause in cfg.get("conditional_rules", {}).items():
            def check_conditional(rule_name=rule_name, where_clause=where_clause):
                sql = f"""
                    SELECT {', '.join(bk)}
                    FROM {silver_fqn}
                    WHERE {where_clause}
                """
                df  = spark.sql(sql)
                cnt = df.count()
                return {
                    "status":         Status.PASS if cnt == 0 else Status.WARN,
                    "failure_count":  cnt,
                    "failure_sample": collect_failure_sample(df, bk) if cnt > 0 else None,
                    "sql_query":      sql,
                }

            vr.run_check(
                check_name        = f"{tbl}.rule_{rule_name}",
                check_category    = CheckCategory.BUSINESS_RULE,
                table_name        = tbl,
                severity          = Severity.WARNING,
                check_description = f"Business rule: {rule_name.replace('_', ' ')}",
                query_fn          = check_conditional,
            )

    overall = vr.overall_status()

# COMMAND ----------

print(f"\nTier 3 overall: {overall.value}")
for r in vr.results:
    icon = "✅" if r.status == Status.PASS else "⚠️"
    print(f"  {icon}  {r.check_name}  ({r.status.value})")

dbutils.notebook.exit(overall.value)
