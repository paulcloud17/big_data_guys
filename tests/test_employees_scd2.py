"""Unit tests for the `silver_employees_scd2` SCD Type 2 transformation."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark_jobs.common.spark_session import join_path
from spark_jobs.silver.transform_employees_scd2 import run as run_employees_scd2

BRONZE_EMPLOYEES_SCHEMA = StructType(
    [
        StructField("emp_id", StringType(), nullable=True),
        StructField("full_name", StringType(), nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("role", StringType(), nullable=True),
        StructField("seniority_level", StringType(), nullable=True),
        StructField("hourly_rate_eur", DoubleType(), nullable=True),
        StructField("hire_date", DateType(), nullable=True),
        StructField("last_promotion_date", DateType(), nullable=True),
        StructField("work_mode", StringType(), nullable=True),
        StructField("distance_km", DoubleType(), nullable=True),
        StructField("manager_id", StringType(), nullable=True),
        StructField("is_active", BooleanType(), nullable=True),
        StructField("termination_date", DateType(), nullable=True),
        StructField("churn_type", StringType(), nullable=True),
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("_batch_date", DateType(), nullable=False),
        StructField("_ingested_at", TimestampType(), nullable=False),
        StructField("_source_file", StringType(), nullable=False),
    ]
)


def _make_bronze_employee_row(
    *,
    batch_date: date,
    emp_id: str = "EMP_0496",
    department: str = "Consulting",
    role: str = "Solutions Consultant",
    hourly_rate_eur: float = 48.79,
    is_active: bool = True,
    termination_date: date | None = None,
    churn_type: str | None = None,
) -> tuple:
    return (
        emp_id,
        "Michelle Beltran",
        department,
        role,
        "Mid",
        hourly_rate_eur,
        date(2025, 8, 24),
        date(2025, 10, 6),
        "Remote",
        22.7,
        "EMP_0324",
        is_active,
        termination_date,
        churn_type,
        batch_date,
        batch_date,
        datetime(2026, 1, 1, 6, 0, tzinfo=timezone.utc),
        f"file:///landing/employees/date={batch_date.isoformat()}/part-000.csv",
    )


def _write_bronze_employees(spark: SparkSession, lakehouse_root: Path, rows: list[tuple]) -> None:
    bronze_path = join_path(str(lakehouse_root), "bronze", "employees")
    df = spark.createDataFrame(rows, schema=BRONZE_EMPLOYEES_SCHEMA)
    (df.write.format("delta").mode("overwrite").partitionBy("_batch_date").save(bronze_path))


def test_silver_employees_scd2_rate_change_and_idempotency(
    spark: SparkSession, tmp_path: Path
) -> None:
    lakehouse_root = tmp_path / "lakehouse"
    _write_bronze_employees(
        spark,
        lakehouse_root,
        [
            _make_bronze_employee_row(batch_date=date(2026, 1, 1), hourly_rate_eur=48.79),
            _make_bronze_employee_row(batch_date=date(2026, 1, 2), hourly_rate_eur=48.79),
            _make_bronze_employee_row(batch_date=date(2026, 1, 3), hourly_rate_eur=54.17),
        ],
    )

    run_employees_scd2(
        spark, batch_date="2026-01-01", lakehouse_root=str(lakehouse_root), run_id="run-1"
    )
    run_employees_scd2(
        spark, batch_date="2026-01-02", lakehouse_root=str(lakehouse_root), run_id="run-2"
    )
    run_employees_scd2(
        spark, batch_date="2026-01-03", lakehouse_root=str(lakehouse_root), run_id="run-3"
    )
    # Rerun January 3 to verify idempotency.
    run_employees_scd2(
        spark, batch_date="2026-01-03", lakehouse_root=str(lakehouse_root), run_id="run-3-rerun"
    )

    silver_path = join_path(str(lakehouse_root), "silver", "silver_employees_scd2")
    rows = (
        spark.read.format("delta")
        .load(silver_path)
        .orderBy("valid_from")
        .select("emp_id", "hourly_rate_eur", "valid_from", "valid_to", "is_current")
        .collect()
    )

    assert len(rows) == 2
    assert (
        rows[0]["emp_id"],
        rows[0]["hourly_rate_eur"],
        rows[0]["valid_from"],
        rows[0]["valid_to"],
        rows[0]["is_current"],
    ) == ("EMP_0496", 48.79, date(2026, 1, 1), date(2026, 1, 3), False)
    assert (
        rows[1]["emp_id"],
        rows[1]["hourly_rate_eur"],
        rows[1]["valid_from"],
        rows[1]["valid_to"],
        rows[1]["is_current"],
    ) == ("EMP_0496", 54.17, date(2026, 1, 3), None, True)


def test_silver_employees_scd2_tracks_null_transitions(spark: SparkSession, tmp_path: Path) -> None:
    lakehouse_root = tmp_path / "lakehouse"
    _write_bronze_employees(
        spark,
        lakehouse_root,
        [
            _make_bronze_employee_row(
                batch_date=date(2026, 1, 1),
                is_active=True,
                termination_date=None,
                churn_type=None,
            ),
            # NULL -> non-NULL transition on termination_date & churn_type
            _make_bronze_employee_row(
                batch_date=date(2026, 1, 2),
                is_active=False,
                termination_date=date(2026, 1, 2),
                churn_type="voluntary",
            ),
            # non-NULL -> NULL transition on churn_type
            _make_bronze_employee_row(
                batch_date=date(2026, 1, 3),
                is_active=False,
                termination_date=date(2026, 1, 2),
                churn_type=None,
            ),
        ],
    )

    for bd in ("2026-01-01", "2026-01-02", "2026-01-03", "2026-01-03"):
        run_employees_scd2(spark, batch_date=bd, lakehouse_root=str(lakehouse_root))

    silver_path = join_path(str(lakehouse_root), "silver", "silver_employees_scd2")
    rows = (
        spark.read.format("delta")
        .load(silver_path)
        .orderBy("valid_from")
        .select(
            "is_active", "termination_date", "churn_type", "valid_from", "valid_to", "is_current"
        )
        .collect()
    )

    assert len(rows) == 3
    assert tuple(rows[0]) == (True, None, None, date(2026, 1, 1), date(2026, 1, 2), False)
    assert tuple(rows[1]) == (
        False,
        date(2026, 1, 2),
        "voluntary",
        date(2026, 1, 2),
        date(2026, 1, 3),
        False,
    )
    assert tuple(rows[2]) == (False, date(2026, 1, 2), None, date(2026, 1, 3), None, True)
