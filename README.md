# Silver Layer Validation Suite

A complete, four-tier validation framework for Databricks Medallion Architecture
Silver layers that use SCD Type 2 and a modified-date watermark ingestion pattern.

---

## File structure

```
silver_validation/
├── 00_validation_orchestrator.py   # Entry point — runs all tiers in order
├── 01_validation_log_setup.py      # Idempotent DDL for log tables
├── 02_tier1_completeness.py        # Bronze → Silver row/key/sum checks (BLOCKING)
├── 03_tier2_scd2_integrity.py      # SCD2 structural correctness (BLOCKING)
├── 04_tier3_business_rules.py      # Domain rules, nulls, ranges, FKs (WARNING)
├── 05_tier4_source_recon.py        # Periodic source vs Silver recon (WARNING)
├── 06_validation_dashboard_queries.py  # Reference SQL for dashboards
├── validation_helpers.py           # Shared library — ValidationRunner, CheckResult
└── README.md
```

---

## Validation log schema

Three Delta tables created in your `validation_schema`:

### `validation_run`
One row per tier per pipeline execution.

| Column | Type | Description |
|---|---|---|
| run_id | STRING | UUID for this tier run |
| pipeline_run_id | STRING | ADF / Workflows job run ID |
| batch_id | STRING | Watermark batch being validated |
| table_name | STRING | Target table or 'ALL' |
| tier | TINYINT | 1–4 |
| tier_name | STRING | Human-readable tier label |
| started_at | TIMESTAMP | When the tier notebook started |
| completed_at | TIMESTAMP | When it finished |
| status | STRING | PASS / FAIL / WARN / RUNNING / ERROR |
| total_checks | INT | Number of individual checks run |
| passed_checks | INT | |
| failed_checks | INT | |
| warned_checks | INT | |
| error_message | STRING | Set if the notebook itself threw |
| run_by | STRING | Notebook path or job name |
| env | STRING | dev / test / prod |

### `validation_check`
One row per individual check.

| Column | Type | Description |
|---|---|---|
| check_id | STRING | UUID |
| run_id | STRING | FK → validation_run |
| pipeline_run_id | STRING | |
| batch_id | STRING | |
| checked_at | TIMESTAMP | |
| tier | TINYINT | |
| tier_name | STRING | |
| table_name | STRING | Silver table checked |
| check_name | STRING | Unique dotted name e.g. `order.scd2_no_overlapping_dates` |
| check_category | STRING | COMPLETENESS / SCD2 / BUSINESS_RULE / SOURCE_RECON |
| check_description | STRING | Plain-English description |
| status | STRING | PASS / FAIL / WARN |
| severity | STRING | BLOCKING / WARNING / INFO |
| expected_value | DOUBLE | Numeric expected (count, sum, %) |
| actual_value | DOUBLE | Numeric actual |
| variance_value | DOUBLE | actual − expected |
| variance_pct | DOUBLE | % deviation |
| failure_count | LONG | Number of offending rows |
| failure_sample | STRING | JSON array of up to 10 bad row keys |
| sql_query | STRING | The exact query run (for audit/debug) |
| execution_ms | LONG | Milliseconds taken |

### `validation_source_recon_snapshot`
Historical trend table for Tier 4 source reconciliation.

| Column | Type | Description |
|---|---|---|
| snapshot_id | STRING | UUID |
| run_id | STRING | FK → validation_run |
| snapshot_date | DATE | |
| table_name | STRING | |
| metric_name | STRING | e.g. `row_count`, `sum_order_total` |
| source_value | DOUBLE | Value from source SQL Server |
| silver_value | DOUBLE | Value from Silver current records |
| variance_value | DOUBLE | |
| variance_pct | DOUBLE | |
| within_threshold | BOOLEAN | |
| threshold_pct | DOUBLE | Configured tolerance |

---

## Quick start

### 1. Upload notebooks to Databricks Workspace
Upload all `.py` files to a single Workspace folder, e.g.
`/Workspace/Shared/silver_validation/`

### 2. Customise the table registries
Each tier notebook has a `TABLE_REGISTRY` dict near the top.  
Edit it to match your Silver tables, business keys, and column names.

### 3. Store source credentials in Databricks Secrets
```bash
databricks secrets create-scope --scope kv-scope
databricks secrets put --scope kv-scope --key sql-server-user
databricks secrets put --scope kv-scope --key sql-server-password
```

### 4. Add to your pipeline
In Databricks Workflows, add a **notebook task** pointing to
`00_validation_orchestrator` after your Silver load tasks.

Recommended widget defaults for the job:
```
catalog             = main
bronze_schema       = bronze
silver_schema       = silver
validation_schema   = validation
pipeline_run_id     = {{job.run_id}}
batch_id            = {{tasks.silver_load.values.batch_id}}
run_source_recon    = false
variance_threshold_pct = 1.0
```

### 5. Schedule Tier 4 separately
Create a second job that calls the orchestrator with `run_source_recon = true`
on a nightly or weekly schedule, off-peak.

### 6. Build a dashboard
Use `06_validation_dashboard_queries.py` as the basis for a Databricks SQL
Dashboard.  Key panels to build:
- Recent run status by tier (table or heatmap)
- Daily pass-rate trend line
- Open FAIL/WARN checks (table, refreshed on schedule)
- Source recon drift trend (line chart per metric per table)

---

## Failure response matrix

| Tier | Severity | Pipeline behaviour | Who acts |
|---|---|---|---|
| 1 — Completeness | BLOCKING | Exception raised, Gold load halted | On-call engineer |
| 2 — SCD2 Integrity | BLOCKING | Exception raised, Gold load halted | On-call engineer (data corruption risk) |
| 3 — Business Rules | WARNING | Logged, pipeline continues | Data steward reviews next business day |
| 4 — Source Recon | WARNING | Logged, pipeline continues | Data steward reviews trend; escalate if persistent |

---

## Adding a new table

1. Add an entry to `TABLE_REGISTRY` in each of the four tier notebooks.
2. Add entries for Bronze in `02_tier1_completeness.py` → `TABLE_REGISTRY`.
3. Add SCD2 column names in `03_tier2_scd2_integrity.py` → `TABLE_REGISTRY`
   (most tables share `SCD2_DEFAULTS`; override only where columns differ).
4. Add domain rules in `04_tier3_business_rules.py` → `TABLE_REGISTRY`.
5. Add source recon metrics in `05_tier4_source_recon.py` → `TABLE_REGISTRY`.

---

## Adding a new business rule check

In `04_tier3_business_rules.py`, add an entry to the table's `conditional_rules` dict:

```python
"conditional_rules": {
    "your_rule_name": """
        is_current = true
        AND <your WHERE clause that should return ZERO rows if healthy>
    """,
}
```

The framework handles timing, logging, and failure sampling automatically.
