# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Tier 4: Source Reconciliation (Periodic)
# MAGIC
# MAGIC Compares Silver current-record aggregates directly against the source
# MAGIC SQL Server to detect drift that Bronze-to-Silver checks cannot catch:
# MAGIC   - Hard deletes in the source (never seen by the watermark pipeline)
# MAGIC   - Backdated updates that were missed by the modified_date filter
# MAGIC   - Cumulative ingestion gaps
# MAGIC
# MAGIC This notebook should be scheduled separately from the main pipeline
# MAGIC (e.g. nightly off-peak or weekly).  It does NOT block any loads.
# MAGIC Results are written to validation_source_recon_snapshot for trending.

# COMMAND ----------

# MAGIC %run ./validation_helpers

# COMMAND ----------

catalog             = dbutils.widgets.get("catalog")
silver_schema       = dbutils.widgets.get("silver_schema")
validation_schema   = dbutils.widgets.get("validation_schema")
table_name          = dbutils.widgets.get("table_name")
pipeline_run_id     = dbutils.widgets.get("pipeline_run_id")
batch_id            = dbutils.widgets.get("batch_id")
variance_threshold  = float(dbutils.widgets.get("variance_threshold_pct"))

# COMMAND ----------

# MAGIC %md ## Source connection
# MAGIC
# MAGIC Replace the JDBC URL, credentials, and driver with your environment's
# MAGIC values.  In production, read the secret from Databricks Secrets:
# MAGIC   password = dbutils.secrets.get(scope="kv-scope", key="sql-server-password")

# COMMAND ----------

SOURCE_JDBC_URL = (
    "jdbc:sqlserver://<your-server>.database.windows.net:1433;"
    "database=<your-db>;"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "loginTimeout=30"
)
SOURCE_JDBC_PROPS = {
    "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "user":     dbutils.secrets.get(scope="kv-scope", key="sql-server-user"),
    "password": dbutils.secrets.get(scope="kv-scope", key="sql-server-password"),
}

def read_source_sql(query: str):
    """Execute a query against the source SQL Server and return a Spark DataFrame."""
    return (
        spark.read.format("jdbc")
        .option("url", SOURCE_JDBC_URL)
        .option("query", query)
        .options(**SOURCE_JDBC_PROPS)
        .load()
    )

# COMMAND ----------

# MAGIC %md ## Table registry — source reconciliation config

# COMMAND ----------

# source_table:    The exact table name in the source SQL Server schema
# source_schema:   The source SQL Server schema (e.g. dbo)
# business_key:    Used to count distinct active entities
# recon_metrics:   List of {name, source_expr, silver_expr} for numeric columns
#                  source_expr  = SQL expression evaluated against source table
#                  silver_expr  = SQL expression evaluated against Silver current records

TABLE_REGISTRY = {
    "customer": {
        "source_schema": "dbo",
        "source_table":  "Customer",
        "business_key":  ["customer_id"],
        "recon_metrics": [
            {
                "name":         "row_count",
                "source_expr":  "COUNT(*)",
                "silver_expr":  "COUNT(*)",
            },
        ],
    },
    "order": {
        "source_schema": "dbo",
        "source_table":  "SalesOrder",
        "business_key":  ["order_id"],
        "recon_metrics": [
            {
                "name":        "row_count",
                "source_expr": "COUNT(*)",
                "silver_expr": "COUNT(*)",
            },
            {
                "name":        "sum_order_total",
                "source_expr": "SUM(OrderTotal)",
                "silver_expr": "SUM(order_total)",
            },
            {
                "name":        "sum_tax_amount",
                "source_expr": "SUM(TaxAmount)",
                "silver_expr": "SUM(tax_amount)",
            },
        ],
    },
    # Add your tables here ...
}

tables_to_check = (
    {table_name: TABLE_REGISTRY[table_name]}
    if table_name and table_name in TABLE_REGISTRY
    else TABLE_REGISTRY
)

# COMMAND ----------

# MAGIC %md ## Run reconciliation

# COMMAND ----------

import uuid
from datetime import date
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    DoubleType, BooleanType
)

snapshot_rows = []

with ValidationRunner(
    spark             = spark,
    catalog           = catalog,
    validation_schema = validation_schema,
    tier              = 4,
    tier_name         = "Source Reconciliation",
    pipeline_run_id   = pipeline_run_id,
    batch_id          = batch_id,
    run_by            = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
) as vr:

    today = date.today()

    for tbl, cfg in tables_to_check.items():
        silver_fqn    = f"{catalog}.{silver_schema}.{tbl}"
        src_fqn       = f"{cfg['source_schema']}.{cfg['source_table']}"

        for metric in cfg.get("recon_metrics", []):
            metric_name  = metric["name"]
            source_expr  = metric["source_expr"]
            silver_expr  = metric["silver_expr"]

            def check_recon(
                tbl=tbl, src_fqn=src_fqn,
                metric_name=metric_name,
                source_expr=source_expr,
                silver_expr=silver_expr,
            ):
                # Query source
                src_val = read_source_sql(
                    f"SELECT {source_expr} AS val FROM {src_fqn}"
                ).collect()[0]["val"]

                # Query Silver
                slv_val = spark.sql(f"""
                    SELECT {silver_expr} AS val
                    FROM {silver_fqn}
                    WHERE is_current = true
                """).collect()[0]["val"]

                src_val = float(src_val) if src_val is not None else 0.0
                slv_val = float(slv_val) if slv_val is not None else 0.0
                variance = slv_val - src_val
                pct      = (variance / src_val * 100) if src_val != 0 else 0.0
                within   = abs(pct) <= variance_threshold

                # Buffer for snapshot table
                snapshot_rows.append((
                    str(uuid.uuid4()),
                    vr.run_id,
                    today,
                    tbl,
                    metric_name,
                    src_val,
                    slv_val,
                    variance,
                    pct,
                    within,
                    variance_threshold,
                ))

                return {
                    "status":         Status.PASS if within else Status.WARN,
                    "expected_value": src_val,
                    "actual_value":   slv_val,
                    "variance_value": variance,
                    "variance_pct":   pct,
                    "sql_query":      f"SOURCE: SELECT {source_expr} FROM {src_fqn}",
                }

            vr.run_check(
                check_name        = f"{tbl}.source_recon_{metric_name}",
                check_category    = CheckCategory.SOURCE_RECON,
                table_name        = tbl,
                severity          = Severity.WARNING,
                check_description = (
                    f"Silver {metric_name} for current records must be within "
                    f"{variance_threshold}% of source value."
                ),
                query_fn          = check_recon,
            )

    overall = vr.overall_status()

# COMMAND ----------

# MAGIC %md ## Write snapshot rows for trend analysis

# COMMAND ----------

if snapshot_rows:
    schema = StructType([
        StructField("snapshot_id",       StringType(),  False),
        StructField("run_id",            StringType(),  False),
        StructField("snapshot_date",     DateType(),    False),
        StructField("table_name",        StringType(),  False),
        StructField("metric_name",       StringType(),  False),
        StructField("source_value",      DoubleType(),  True),
        StructField("silver_value",      DoubleType(),  True),
        StructField("variance_value",    DoubleType(),  True),
        StructField("variance_pct",      DoubleType(),  True),
        StructField("within_threshold",  BooleanType(), True),
        StructField("threshold_pct",     DoubleType(),  True),
    ])

    snap_df = spark.createDataFrame(snapshot_rows, schema)
    snap_df.write.format("delta").mode("append").saveAsTable(
        f"{catalog}.{validation_schema}.validation_source_recon_snapshot"
    )
    print(f"Wrote {len(snapshot_rows)} snapshot rows.")

# COMMAND ----------

print(f"\nTier 4 overall: {overall.value}")
for r in vr.results:
    icon = "✅" if r.status == Status.PASS else "⚠️"
    pct  = f"  variance={r.variance_pct:.2f}%" if r.variance_pct is not None else ""
    print(f"  {icon}  {r.check_name}  ({r.status.value}){pct}")

dbutils.notebook.exit(overall.value)
