"""Unit tests for Bronze HR (`employees` & `timesheets`) ingestion."""

from __future__ import annotations

import shutil
from datetime import date
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

from spark_jobs.bronze.ingest_hr import EMPLOYEES_RAW_SCHEMA
from spark_jobs.bronze.ingest_hr import run as run_ingest_hr
from spark_jobs.common.spark_session import join_path

EMPLOYEE_HEADER = ",".join(field.name for field in EMPLOYEES_RAW_SCHEMA.fields)

TIMESHEET_RAW_SCHEMA = StructType(
    [
        StructField("ts_id", StringType(), nullable=True),
        StructField("emp_id", StringType(), nullable=True),
        StructField("project_id", StringType(), nullable=True),
        StructField("work_date", DateType(), nullable=True),
        StructField("hours", DoubleType(), nullable=True),
    ]
)


def _write_employees_csv(landing_root: Path, batch_date: str, rows: list[str]) -> None:
    part_dir = landing_root / "employees" / f"date={batch_date}"
    part_dir.mkdir(parents=True, exist_ok=True)
    content = "\n".join([EMPLOYEE_HEADER, *rows]) + "\n"
    (part_dir / "part-000.csv").write_text(content, encoding="utf-8")


def _write_timesheets_parquet(
    spark: SparkSession, landing_root: Path, batch_date: str, rows: list[tuple]
) -> None:
    staging_dir = landing_root / "_staging_timesheets"
    df = spark.createDataFrame(rows, schema=TIMESHEET_RAW_SCHEMA)
    df.coalesce(1).write.mode("overwrite").parquet(str(staging_dir))
    parquet_files = list(staging_dir.glob("*.parquet"))
    assert len(parquet_files) == 1

    part_dir = landing_root / "timesheets" / f"date={batch_date}"
    part_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(parquet_files[0], part_dir / "part-000.parquet")
    shutil.rmtree(staging_dir)


def _emp_row(emp_id: str, rate: str, snapshot_date: str) -> str:
    return (
        f"{emp_id},Michelle Beltran,Consulting,Solutions Consultant,Mid,{rate},"
        f"2025-08-24,2025-10-06,Remote,22.7,EMP_0324,true,,,{snapshot_date}"
    )


def test_ingest_hr_bronze_replace_where_duplicates_and_metadata(
    spark: SparkSession, tmp_path: Path
) -> None:
    landing_root = tmp_path / "landing"
    lakehouse_root = tmp_path / "lakehouse"

    # January 1 landing files
    _write_employees_csv(
        landing_root,
        "2026-01-01",
        [
            _emp_row("EMP_0496", "48.79", "2026-01-01"),
            _emp_row("EMP_0002", "60.00", "2026-01-01"),
        ],
    )
    _write_timesheets_parquet(
        spark,
        landing_root,
        "2026-01-01",
        [
            ("TS-20260101-0001", "EMP_0496", "PRJ_0001", date(2026, 1, 1), 8.0),
            ("TS-20260101-0002", "EMP_0002", None, date(2026, 1, 1), 4.5),
        ],
    )

    # January 2 landing files (including exact duplicate rows in both datasets)
    _write_employees_csv(
        landing_root,
        "2026-01-02",
        [
            _emp_row("EMP_0496", "48.79", "2026-01-02"),
            _emp_row("EMP_0002", "60.00", "2026-01-02"),
            _emp_row("EMP_0002", "60.00", "2026-01-02"),
        ],
    )
    _write_timesheets_parquet(
        spark,
        landing_root,
        "2026-01-02",
        [
            ("TS-20260102-0001", "EMP_0496", "PRJ_0001", date(2026, 1, 2), 7.5),
            ("TS-20260102-0002", "EMP_0002", "PRJ_0002", date(2026, 1, 2), 6.0),
            ("TS-20260102-0002", "EMP_0002", "PRJ_0002", date(2026, 1, 2), 6.0),
        ],
    )

    # Run January 1 and January 2
    run_ingest_hr(
        spark,
        batch_date="2026-01-01",
        landing_root=str(landing_root),
        lakehouse_root=str(lakehouse_root),
    )
    run_ingest_hr(
        spark,
        batch_date="2026-01-02",
        landing_root=str(landing_root),
        lakehouse_root=str(lakehouse_root),
    )

    # Correct one January 2 input value in each dataset and rerun January 2
    _write_employees_csv(
        landing_root,
        "2026-01-02",
        [
            _emp_row("EMP_0496", "54.17", "2026-01-02"),
            _emp_row("EMP_0002", "60.00", "2026-01-02"),
            _emp_row("EMP_0002", "60.00", "2026-01-02"),
        ],
    )
    _write_timesheets_parquet(
        spark,
        landing_root,
        "2026-01-02",
        [
            ("TS-20260102-0001", "EMP_0496", "PRJ_0001", date(2026, 1, 2), 8.25),
            ("TS-20260102-0002", "EMP_0002", "PRJ_0002", date(2026, 1, 2), 6.0),
            ("TS-20260102-0002", "EMP_0002", "PRJ_0002", date(2026, 1, 2), 6.0),
        ],
    )

    run_ingest_hr(
        spark,
        batch_date="2026-01-02",
        landing_root=str(landing_root),
        lakehouse_root=str(lakehouse_root),
    )

    # --- Verify bronze/employees ---
    employees_df = spark.read.format("delta").load(
        join_path(str(lakehouse_root), "bronze", "employees")
    )
    for field in EMPLOYEES_RAW_SCHEMA.fields:
        assert isinstance(employees_df.schema[field.name].dataType, StringType)

    assert isinstance(employees_df.schema["_batch_date"].dataType, DateType)
    assert isinstance(employees_df.schema["_ingested_at"].dataType, TimestampType)
    assert isinstance(employees_df.schema["_source_file"].dataType, StringType)
    assert (
        employees_df.filter(
            "_batch_date IS NULL OR _ingested_at IS NULL OR _source_file IS NULL"
        ).count()
        == 0
    )

    # 2 rows from Jan 1 + 3 rows from Jan 2 (duplicate preserved, no extra rows on rerun)
    assert employees_df.count() == 5
    emp_rows = employees_df.select(
        "emp_id", "hourly_rate_eur", "_batch_date", "_source_file"
    ).collect()

    jan1_emp = [r for r in emp_rows if r["_batch_date"] == date(2026, 1, 1)]
    jan2_emp = [r for r in emp_rows if r["_batch_date"] == date(2026, 1, 2)]

    assert sorted((r["emp_id"], r["hourly_rate_eur"]) for r in jan1_emp) == [
        ("EMP_0002", "60.00"),
        ("EMP_0496", "48.79"),
    ]
    assert sorted((r["emp_id"], r["hourly_rate_eur"]) for r in jan2_emp) == [
        ("EMP_0002", "60.00"),
        ("EMP_0002", "60.00"),
        ("EMP_0496", "54.17"),
    ]
    assert all(
        r["_source_file"].endswith("/employees/date=2026-01-01/part-000.csv") for r in jan1_emp
    )
    assert all(
        r["_source_file"].endswith("/employees/date=2026-01-02/part-000.csv") for r in jan2_emp
    )

    # --- Verify bronze/timesheets ---
    timesheets_df = spark.read.format("delta").load(
        join_path(str(lakehouse_root), "bronze", "timesheets")
    )
    assert isinstance(timesheets_df.schema["ts_id"].dataType, StringType)
    assert isinstance(timesheets_df.schema["emp_id"].dataType, StringType)
    assert isinstance(timesheets_df.schema["project_id"].dataType, StringType)
    assert isinstance(timesheets_df.schema["work_date"].dataType, DateType)
    assert isinstance(timesheets_df.schema["hours"].dataType, DoubleType)
    assert isinstance(timesheets_df.schema["_batch_date"].dataType, DateType)
    assert isinstance(timesheets_df.schema["_ingested_at"].dataType, TimestampType)
    assert isinstance(timesheets_df.schema["_source_file"].dataType, StringType)
    assert (
        timesheets_df.filter(
            "_batch_date IS NULL OR _ingested_at IS NULL OR _source_file IS NULL"
        ).count()
        == 0
    )

    # 2 rows from Jan 1 + 3 rows from Jan 2 (duplicate preserved, no extra rows on rerun)
    assert timesheets_df.count() == 5
    ts_rows = timesheets_df.select("ts_id", "hours", "_batch_date", "_source_file").collect()

    jan1_ts = [r for r in ts_rows if r["_batch_date"] == date(2026, 1, 1)]
    jan2_ts = [r for r in ts_rows if r["_batch_date"] == date(2026, 1, 2)]

    assert sorted((r["ts_id"], r["hours"]) for r in jan1_ts) == [
        ("TS-20260101-0001", 8.0),
        ("TS-20260101-0002", 4.5),
    ]
    assert sorted((r["ts_id"], r["hours"]) for r in jan2_ts) == [
        ("TS-20260102-0001", 8.25),
        ("TS-20260102-0002", 6.0),
        ("TS-20260102-0002", 6.0),
    ]
    assert all(
        r["_source_file"].endswith("/timesheets/date=2026-01-01/part-000.parquet") for r in jan1_ts
    )
    assert all(
        r["_source_file"].endswith("/timesheets/date=2026-01-02/part-000.parquet") for r in jan2_ts
    )
