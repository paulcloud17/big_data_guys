"""Unit tests for `silver_timesheets` cleaning, deduplication, quarantine, and idempotency."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_jobs.common.spark_session import join_path
from spark_jobs.silver.transform_timesheets import run as run_timesheets_silver

BRONZE_TIMESHEETS_SCHEMA = StructType(
    [
        StructField("ts_id", StringType(), nullable=True),
        StructField("emp_id", StringType(), nullable=True),
        StructField("project_id", StringType(), nullable=True),
        StructField("work_date", DateType(), nullable=True),
        StructField("hours", DoubleType(), nullable=True),
        StructField("_batch_date", DateType(), nullable=False),
        StructField("_ingested_at", TimestampType(), nullable=False),
        StructField("_source_file", StringType(), nullable=False),
    ]
)

BRONZE_EMPLOYEES_SCHEMA = StructType(
    [
        StructField("emp_id", StringType(), nullable=True),
        StructField("termination_date", StringType(), nullable=True),
        StructField("_batch_date", DateType(), nullable=False),
    ]
)


def _ts_row(
    *,
    ts_id: str | None,
    emp_id: str | None,
    project_id: str | None,
    work_date: date | None,
    hours: float | None,
    batch_date: date,
    ingested_at: datetime | None = None,
) -> tuple:
    ts = ingested_at or datetime(2026, 1, 1, 6, 0, tzinfo=timezone.utc)
    return (
        ts_id,
        emp_id,
        project_id,
        work_date,
        hours,
        batch_date,
        ts,
        f"file:///landing/timesheets/date={batch_date.isoformat()}/part-000.parquet",
    )


def _write_bronze_timesheets(spark: SparkSession, lakehouse_root: Path, rows: list[tuple]) -> None:
    bronze_path = join_path(str(lakehouse_root), "bronze", "timesheets")
    df = spark.createDataFrame(rows, schema=BRONZE_TIMESHEETS_SCHEMA)
    (df.write.format("delta").mode("overwrite").partitionBy("_batch_date").save(bronze_path))


def _write_bronze_employees(spark: SparkSession, lakehouse_root: Path, rows: list[tuple]) -> None:
    bronze_path = join_path(str(lakehouse_root), "bronze", "employees")
    df = spark.createDataFrame(rows, schema=BRONZE_EMPLOYEES_SCHEMA)
    (df.write.format("delta").mode("overwrite").partitionBy("_batch_date").save(bronze_path))


def test_silver_timesheets_dedup_tie_breaker_and_cleaning(
    spark: SparkSession, tmp_path: Path
) -> None:
    lakehouse_root = tmp_path / "lakehouse"
    d1 = date(2026, 1, 1)

    _write_bronze_timesheets(
        spark,
        lakehouse_root,
        [
            # Exact duplicate -> collapses to 1 cleaned row
            _ts_row(
                ts_id=" TS-EXACT ",
                emp_id=" EMP_0001 ",
                project_id=" PRJ_0001 ",
                work_date=d1,
                hours=4.0,
                batch_date=d1,
            ),
            _ts_row(
                ts_id="TS-EXACT",
                emp_id="EMP_0001",
                project_id="PRJ_0001",
                work_date=d1,
                hours=4.0,
                batch_date=d1,
            ),
            # Near-duplicate with two valid positive hours -> lowest valid hours (1.75) survives
            _ts_row(
                ts_id="TS-NEAR-VALID",
                emp_id="EMP_0010",
                project_id="PRJ_0464",
                work_date=d1,
                hours=3.25,
                batch_date=d1,
            ),
            _ts_row(
                ts_id="TS-NEAR-VALID",
                emp_id="EMP_0010",
                project_id="PRJ_0464",
                work_date=d1,
                hours=1.75,
                batch_date=d1,
            ),
            # Near-duplicate with invalid negative vs. valid positive -> valid (5.0) survives
            _ts_row(
                ts_id="TS-NEAR-MIXED",
                emp_id="EMP_0011",
                project_id="PRJ_0465",
                work_date=d1,
                hours=-2.0,
                batch_date=d1,
            ),
            _ts_row(
                ts_id="TS-NEAR-MIXED",
                emp_id="EMP_0011",
                project_id="PRJ_0465",
                work_date=d1,
                hours=5.0,
                batch_date=d1,
            ),
        ],
    )

    run_timesheets_silver(
        spark, batch_date="2026-01-01", lakehouse_root=str(lakehouse_root), run_id="test-dedup"
    )

    silver_df = spark.read.format("delta").load(
        join_path(str(lakehouse_root), "silver", "silver_timesheets")
    )
    quarantine_df = spark.read.format("delta").load(
        join_path(str(lakehouse_root), "silver_quarantine", "timesheets")
    )

    assert quarantine_df.count() == 0
    rows = {
        r["ts_id"]: (r["emp_id"], r["project_id"], r["hours"])
        for r in silver_df.select("ts_id", "emp_id", "project_id", "hours").collect()
    }
    assert rows == {
        "TS-EXACT": ("EMP_0001", "PRJ_0001", 4.0),
        "TS-NEAR-VALID": ("EMP_0010", "PRJ_0464", 1.75),
        "TS-NEAR-MIXED": ("EMP_0011", "PRJ_0465", 5.0),
    }


def test_silver_timesheets_quarantine_reasons_and_idempotency(
    spark: SparkSession, tmp_path: Path
) -> None:
    lakehouse_root = tmp_path / "lakehouse"
    d1 = date(2026, 1, 5)

    # EMP_TERM was terminated on 2026-01-05 (work_date >= 2026-01-05 is post-termination)
    _write_bronze_employees(
        spark,
        lakehouse_root,
        [
            ("EMP_ACTIVE", "", d1),
            ("EMP_TERM", "2026-01-05", d1),
        ],
    )
    _write_bronze_timesheets(
        spark,
        lakehouse_root,
        [
            # Valid active row
            _ts_row(
                ts_id="TS-OK",
                emp_id="EMP_ACTIVE",
                project_id="PRJ_0001",
                work_date=d1,
                hours=8.0,
                batch_date=d1,
            ),
            # Valid row worked strictly before termination_date
            _ts_row(
                ts_id="TS-BEFORE-TERM",
                emp_id="EMP_TERM",
                project_id="PRJ_0001",
                work_date=date(2026, 1, 4),
                hours=6.0,
                batch_date=d1,
            ),
            # Invalid: NULL project_id
            _ts_row(
                ts_id="TS-NULL-PRJ",
                emp_id="EMP_ACTIVE",
                project_id=None,
                work_date=d1,
                hours=4.0,
                batch_date=d1,
            ),
            # Invalid: hours <= 0
            _ts_row(
                ts_id="TS-NEG-HRS",
                emp_id="EMP_ACTIVE",
                project_id="PRJ_0001",
                work_date=d1,
                hours=0.0,
                batch_date=d1,
            ),
            # Invalid: hours > 24
            _ts_row(
                ts_id="TS-OVER-24",
                emp_id="EMP_ACTIVE",
                project_id="PRJ_0001",
                work_date=d1,
                hours=25.5,
                batch_date=d1,
            ),
            # Invalid: post-termination (work_date == termination_date)
            _ts_row(
                ts_id="TS-ZOMBIE",
                emp_id="EMP_TERM",
                project_id="PRJ_0001",
                work_date=d1,
                hours=7.0,
                batch_date=d1,
            ),
        ],
    )

    # Run twice for the same batch_date to verify idempotency on both outputs.
    run_timesheets_silver(
        spark, batch_date="2026-01-05", lakehouse_root=str(lakehouse_root), run_id="run-1"
    )
    run_timesheets_silver(
        spark, batch_date="2026-01-05", lakehouse_root=str(lakehouse_root), run_id="run-2"
    )

    silver_df = spark.read.format("delta").load(
        join_path(str(lakehouse_root), "silver", "silver_timesheets")
    )
    quarantine_df = spark.read.format("delta").load(
        join_path(str(lakehouse_root), "silver_quarantine", "timesheets")
    )
    audit_df = spark.read.format("delta").load(
        join_path(str(lakehouse_root), "dq", "dq_audit_results")
    )

    # Exactly 2 valid rows (no duplicates after rerun)
    assert silver_df.count() == 2
    assert {r["ts_id"] for r in silver_df.select("ts_id").collect()} == {
        "TS-OK",
        "TS-BEFORE-TERM",
    }

    # Exactly 4 quarantined rows with expected reasons (no duplicates after rerun)
    assert quarantine_df.count() == 4
    reasons_by_id = {
        r["ts_id"]: (r["_batch_date"], list(r["dq_reasons"]))
        for r in quarantine_df.select("ts_id", "_batch_date", "dq_reasons").collect()
    }
    assert reasons_by_id == {
        "TS-NULL-PRJ": (d1, ["missing_project_id"]),
        "TS-NEG-HRS": (d1, ["non_positive_hours"]),
        "TS-OVER-24": (d1, ["hours_exceed_24"]),
        "TS-ZOMBIE": (d1, ["post_termination_entry"]),
    }

    # DQ audit results reflect only the latest rerun for 2026-01-05 (idempotent)
    audit_rows = {
        r["check_name"]: (r["run_id"], r["severity"], r["status"], r["failed_count"])
        for r in audit_df.collect()
    }
    assert audit_rows["not_null_ts_id"] == ("run-2", "ERROR", "PASS", 0)
    assert audit_rows["unique_ts_id"] == ("run-2", "ERROR", "PASS", 0)
    assert audit_rows["quarantine_ratio"] == ("run-2", "WARN", "WARN", 4)
    assert audit_rows["zombie_timesheet_count"] == ("run-2", "WARN", "WARN", 1)
