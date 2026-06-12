# validation_helpers.py
# Import this module at the top of each tier notebook:
#   import sys; sys.path.insert(0, "/Workspace/path/to/notebooks")
#   from validation_helpers import ValidationRunner, CheckResult, Severity, Status
#
# Or in Databricks, use %run ./validation_helpers

import uuid
import json
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Optional, List, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, LongType, IntegerType, ShortType, BooleanType
)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Status(str, Enum):
    PASS    = "PASS"
    FAIL    = "FAIL"
    WARN    = "WARN"
    RUNNING = "RUNNING"
    ERROR   = "ERROR"


class Severity(str, Enum):
    BLOCKING = "BLOCKING"   # Tier 1 & 2 failures — halt the pipeline
    WARNING  = "WARNING"    # Tier 3 & 4 — log and alert, do not halt
    INFO     = "INFO"       # Informational only


class CheckCategory(str, Enum):
    COMPLETENESS  = "COMPLETENESS"
    SCD2          = "SCD2"
    BUSINESS_RULE = "BUSINESS_RULE"
    SOURCE_RECON  = "SOURCE_RECON"


# ---------------------------------------------------------------------------
# Check result dataclass
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    check_name:        str
    check_category:    CheckCategory
    table_name:        str
    status:            Status
    severity:          Severity
    check_description: str              = ""
    expected_value:    Optional[float]  = None
    actual_value:      Optional[float]  = None
    variance_value:    Optional[float]  = None
    variance_pct:      Optional[float]  = None
    failure_count:     Optional[int]    = None
    failure_sample:    Optional[str]    = None   # JSON string
    sql_query:         Optional[str]    = None
    execution_ms:      Optional[int]    = None

    # Computed fields (set by ValidationRunner)
    check_id:          str = field(default_factory=lambda: str(uuid.uuid4()))
    checked_at:        datetime = field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Main runner class
# ---------------------------------------------------------------------------

class ValidationRunner:
    """
    Collects CheckResult objects, writes them to the validation_check Delta
    table, and writes a summary row to validation_run.
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        validation_schema: str,
        tier: int,
        tier_name: str,
        pipeline_run_id: str = "",
        batch_id: str = "",
        run_by: str = "",
        env: str = "dev",
    ):
        self.spark            = spark
        self.catalog          = catalog
        self.validation_schema = validation_schema
        self.tier             = tier
        self.tier_name        = tier_name
        self.pipeline_run_id  = pipeline_run_id
        self.batch_id         = batch_id
        self.run_by           = run_by
        self.env              = env

        self.run_id     = str(uuid.uuid4())
        self.started_at = datetime.utcnow()
        self.results: List[CheckResult] = []

        self._check_table  = f"{catalog}.{validation_schema}.validation_check"
        self._run_table    = f"{catalog}.{validation_schema}.validation_run"

    # -----------------------------------------------------------------------
    # Context manager support  (with ValidationRunner(...) as vr:)
    # -----------------------------------------------------------------------

    def __enter__(self):
        self._write_run_row(Status.RUNNING)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self._write_run_row(Status.ERROR, error_message=str(exc_val))
        else:
            self.flush()
        return False   # Do not suppress exceptions

    # -----------------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------------

    def add(self, result: CheckResult):
        """Add a pre-built CheckResult."""
        self.results.append(result)

    def run_check(
        self,
        check_name: str,
        check_category: CheckCategory,
        table_name: str,
        severity: Severity,
        check_description: str,
        query_fn,          # Callable[[], dict] — returns keys matching CheckResult fields
    ) -> CheckResult:
        """
        Execute a check function, record timing, build a CheckResult, and add it.

        query_fn must return a dict with at minimum:
            status        (Status)
            (any subset of other CheckResult fields)
        """
        t0 = time.time()
        try:
            result_dict = query_fn()
        except Exception as e:
            result_dict = {
                "status":      Status.FAIL,
                "sql_query":   f"ERROR: {e}",
            }

        execution_ms = int((time.time() - t0) * 1000)

        result = CheckResult(
            check_name        = check_name,
            check_category    = check_category,
            table_name        = table_name,
            severity          = severity,
            check_description = check_description,
            execution_ms      = execution_ms,
            **{k: v for k, v in result_dict.items()
               if k in CheckResult.__dataclass_fields__},
        )
        self.add(result)
        return result

    def flush(self):
        """Write all buffered results and update the run summary row."""
        if self.results:
            self._write_check_rows()

        passed  = sum(1 for r in self.results if r.status == Status.PASS)
        failed  = sum(1 for r in self.results if r.status == Status.FAIL)
        warned  = sum(1 for r in self.results if r.status == Status.WARN)

        overall = Status.FAIL if failed > 0 else (Status.WARN if warned > 0 else Status.PASS)
        self._write_run_row(overall, passed=passed, failed=failed, warned=warned)

        return overall

    def overall_status(self) -> Status:
        if any(r.status == Status.FAIL for r in self.results):
            return Status.FAIL
        if any(r.status == Status.WARN for r in self.results):
            return Status.WARN
        return Status.PASS

    # -----------------------------------------------------------------------
    # Private helpers
    # -----------------------------------------------------------------------

    def _write_check_rows(self):
        rows = []
        for r in self.results:
            rows.append((
                r.check_id,
                self.run_id,
                self.pipeline_run_id,
                self.batch_id,
                r.checked_at,
                self.tier,
                self.tier_name,
                r.table_name,
                r.check_name,
                r.check_category.value if isinstance(r.check_category, CheckCategory) else r.check_category,
                r.check_description,
                r.status.value if isinstance(r.status, Status) else r.status,
                r.severity.value if isinstance(r.severity, Severity) else r.severity,
                float(r.expected_value) if r.expected_value is not None else None,
                float(r.actual_value)   if r.actual_value   is not None else None,
                float(r.variance_value) if r.variance_value is not None else None,
                float(r.variance_pct)   if r.variance_pct   is not None else None,
                int(r.failure_count)    if r.failure_count   is not None else None,
                r.failure_sample,
                r.sql_query,
                r.execution_ms,
            ))

        schema = StructType([
            StructField("check_id",          StringType(),    False),
            StructField("run_id",            StringType(),    False),
            StructField("pipeline_run_id",   StringType(),    True),
            StructField("batch_id",          StringType(),    True),
            StructField("checked_at",        TimestampType(), False),
            StructField("tier",              ShortType(),     False),
            StructField("tier_name",         StringType(),    False),
            StructField("table_name",        StringType(),    False),
            StructField("check_name",        StringType(),    False),
            StructField("check_category",    StringType(),    False),
            StructField("check_description", StringType(),    True),
            StructField("status",            StringType(),    False),
            StructField("severity",          StringType(),    False),
            StructField("expected_value",    DoubleType(),    True),
            StructField("actual_value",      DoubleType(),    True),
            StructField("variance_value",    DoubleType(),    True),
            StructField("variance_pct",      DoubleType(),    True),
            StructField("failure_count",     LongType(),      True),
            StructField("failure_sample",    StringType(),    True),
            StructField("sql_query",         StringType(),    True),
            StructField("execution_ms",      LongType(),      True),
        ])

        df = self.spark.createDataFrame(rows, schema)
        df.write.format("delta").mode("append").saveAsTable(self._check_table)

    def _write_run_row(
        self,
        status: Status,
        passed: int = 0,
        failed: int = 0,
        warned: int = 0,
        error_message: str = None,
    ):
        completed_at = datetime.utcnow() if status != Status.RUNNING else None
        total = passed + failed + warned

        rows = [(
            self.run_id,
            self.pipeline_run_id,
            self.batch_id,
            "",           # table_name — set per-check; run row covers the tier
            self.tier,
            self.tier_name,
            self.started_at,
            completed_at,
            status.value if isinstance(status, Status) else status,
            total or None,
            passed or None,
            failed or None,
            warned or None,
            error_message,
            self.run_by,
            self.env,
        )]

        schema = StructType([
            StructField("run_id",          StringType(),    False),
            StructField("pipeline_run_id", StringType(),    True),
            StructField("batch_id",        StringType(),    True),
            StructField("table_name",      StringType(),    True),
            StructField("tier",            ShortType(),     False),
            StructField("tier_name",       StringType(),    False),
            StructField("started_at",      TimestampType(), False),
            StructField("completed_at",    TimestampType(), True),
            StructField("status",          StringType(),    False),
            StructField("total_checks",    IntegerType(),   True),
            StructField("passed_checks",   IntegerType(),   True),
            StructField("failed_checks",   IntegerType(),   True),
            StructField("warned_checks",   IntegerType(),   True),
            StructField("error_message",   StringType(),    True),
            StructField("run_by",          StringType(),    True),
            StructField("env",             StringType(),    True),
        ])

        df = self.spark.createDataFrame(rows, schema)

        # MERGE so that a RUNNING row opened by __enter__ gets updated on exit
        df.createOrReplaceTempView("_vr_update")
        self.spark.sql(f"""
            MERGE INTO {self._run_table} AS target
            USING _vr_update AS source
            ON target.run_id = source.run_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)


# ---------------------------------------------------------------------------
# Convenience: collect a failure sample (up to N row keys as JSON)
# ---------------------------------------------------------------------------

def collect_failure_sample(df: DataFrame, key_cols: List[str], n: int = 10) -> str:
    """Return a JSON string of up to n bad row key values for storage in failure_sample."""
    rows = df.select(*key_cols).limit(n).collect()
    return json.dumps([row.asDict() for row in rows])
