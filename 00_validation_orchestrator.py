# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Validation Orchestrator
# MAGIC
# MAGIC This notebook is the entry point for all Silver layer validations.
# MAGIC It accepts parameters, runs each validation tier in sequence, writes
# MAGIC results to the validation log, and returns a structured summary.
# MAGIC
# MAGIC **Run order:**
# MAGIC 1. `01_validation_log_setup`   — ensure log table exists
# MAGIC 2. `02_tier1_completeness`     — Bronze → Silver row/key coverage
# MAGIC 3. `03_tier2_scd2_integrity`   — SCD2 correctness checks
# MAGIC 4. `04_tier3_business_rules`   — Nulls, ranges, cross-field logic
# MAGIC 5. `05_tier4_source_recon`     — Periodic source reconciliation (optional)
# MAGIC
# MAGIC **Failure behaviour:**
# MAGIC - Tier 1 FAIL  → raises exception, blocks downstream Gold load
# MAGIC - Tier 2 FAIL  → raises exception, pages on-call (wire to your alerting)
# MAGIC - Tier 3 WARN  → logs warning, does NOT block Gold load
# MAGIC - Tier 4 WARN  → logs warning, does NOT block Gold load

# COMMAND ----------

# MAGIC %md ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog",            "main",         "Unity Catalog name")
dbutils.widgets.text("bronze_schema",      "bronze",       "Bronze schema name")
dbutils.widgets.text("silver_schema",      "silver",       "Silver schema name")
dbutils.widgets.text("validation_schema",  "validation",   "Validation log schema")
dbutils.widgets.text("table_name",         "",             "Table to validate (blank = all)")
dbutils.widgets.text("pipeline_run_id",    "",             "Pipeline run ID (from ADF/Workflows)")
dbutils.widgets.text("batch_id",           "",             "Batch ID being validated")
dbutils.widgets.dropdown("run_source_recon", "false", ["true", "false"], "Run Tier 4 source recon?")
dbutils.widgets.text("variance_threshold_pct", "1.0",      "Source recon variance threshold %")

# COMMAND ----------

catalog               = dbutils.widgets.get("catalog")
bronze_schema         = dbutils.widgets.get("bronze_schema")
silver_schema         = dbutils.widgets.get("silver_schema")
validation_schema     = dbutils.widgets.get("validation_schema")
table_name            = dbutils.widgets.get("table_name")
pipeline_run_id       = dbutils.widgets.get("pipeline_run_id")
batch_id              = dbutils.widgets.get("batch_id")
run_source_recon      = dbutils.widgets.get("run_source_recon").lower() == "true"
variance_threshold    = float(dbutils.widgets.get("variance_threshold_pct"))

# COMMAND ----------

# MAGIC %md ## Shared context passed to child notebooks

# COMMAND ----------

# Build the context dict that every child notebook receives via dbutils.jobs.taskValues
# When running as a Workflow, use task values. When running interactively, use widgets.

validation_context = {
    "catalog":             catalog,
    "bronze_schema":       bronze_schema,
    "silver_schema":       silver_schema,
    "validation_schema":   validation_schema,
    "table_name":          table_name,
    "pipeline_run_id":     pipeline_run_id,
    "batch_id":            batch_id,
    "variance_threshold":  variance_threshold,
}

print("Validation context:")
for k, v in validation_context.items():
    print(f"  {k}: {v}")

# COMMAND ----------

# MAGIC %md ## Step 0 — Ensure log infrastructure exists

# COMMAND ----------

dbutils.notebook.run(
    "./01_validation_log_setup",
    timeout_seconds=120,
    arguments=validation_context
)

# COMMAND ----------

# MAGIC %md ## Step 1 — Tier 1: Completeness

# COMMAND ----------

tier1_result = dbutils.notebook.run(
    "./02_tier1_completeness",
    timeout_seconds=600,
    arguments=validation_context
)

tier1_passed = tier1_result == "PASS"
print(f"Tier 1 Completeness: {'✅ PASS' if tier1_passed else '❌ FAIL'}")

if not tier1_passed:
    raise Exception(
        f"[BLOCKING] Tier 1 Completeness validation FAILED for batch '{batch_id}'. "
        "Downstream Gold load has been halted. Check validation_log for details."
    )

# COMMAND ----------

# MAGIC %md ## Step 2 — Tier 2: SCD2 Integrity

# COMMAND ----------

tier2_result = dbutils.notebook.run(
    "./03_tier2_scd2_integrity",
    timeout_seconds=600,
    arguments=validation_context
)

tier2_passed = tier2_result == "PASS"
print(f"Tier 2 SCD2 Integrity: {'✅ PASS' if tier2_passed else '❌ FAIL'}")

if not tier2_passed:
    raise Exception(
        f"[BLOCKING] Tier 2 SCD2 Integrity validation FAILED for batch '{batch_id}'. "
        "This may indicate data corruption. Check validation_log immediately."
    )

# COMMAND ----------

# MAGIC %md ## Step 3 — Tier 3: Business Rules

# COMMAND ----------

tier3_result = dbutils.notebook.run(
    "./04_tier3_business_rules",
    timeout_seconds=600,
    arguments=validation_context
)

tier3_passed = tier3_result == "PASS"
print(f"Tier 3 Business Rules: {'⚠️ WARN' if not tier3_passed else '✅ PASS'}")
# Tier 3 does NOT block — warnings are logged but pipeline continues

# COMMAND ----------

# MAGIC %md ## Step 4 — Tier 4: Source Reconciliation (optional)

# COMMAND ----------

tier4_passed = True

if run_source_recon:
    tier4_result = dbutils.notebook.run(
        "./05_tier4_source_recon",
        timeout_seconds=900,
        arguments=validation_context
    )
    tier4_passed = tier4_result == "PASS"
    print(f"Tier 4 Source Recon: {'⚠️ WARN' if not tier4_passed else '✅ PASS'}")
else:
    print("Tier 4 Source Recon: ⏭️ SKIPPED (set run_source_recon=true to enable)")

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

from datetime import datetime

summary = {
    "batch_id":        batch_id,
    "pipeline_run_id": pipeline_run_id,
    "completed_at":    datetime.utcnow().isoformat(),
    "tier1_pass":      tier1_passed,
    "tier2_pass":      tier2_passed,
    "tier3_pass":      tier3_passed,
    "tier4_pass":      tier4_passed,
    "overall_pass":    all([tier1_passed, tier2_passed]),  # Tier 3/4 are advisory
}

print("\n========== VALIDATION SUMMARY ==========")
for k, v in summary.items():
    print(f"  {k}: {v}")
print("=========================================")

dbutils.notebook.exit("PASS" if summary["overall_pass"] else "FAIL")
