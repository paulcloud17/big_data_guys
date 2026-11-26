"""Bronze ingestion job for HR domain (`employees` CSV and `timesheets` Parquet).

Reads daily Hive-partitioned landing files for the requested `batch_date`:
- `<landing_root>/employees/date=<batch_date>/part-000.csv` (all fields preserved as strings)
- `<landing_root>/timesheets/date=<batch_date>/part-000.parquet` (original Parquet types preserved)

Preserves duplicate and raw rows without lossy transformations, appends
standardized metadata (`_batch_date`, `_ingested_at`, `_source_file`), and
writes idempotently to `bronze/employees` and `bronze/timesheets` partitioned
by `_batch_date` using Delta `replaceWhere`.
"""

from __future__ import annotations

import argparse
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
    join_path,
)

EMPLOYEES_RAW_SCHEMA = StructType(
    [
        StructField("emp_id", StringType(), nullable=True),
        StructField("full_name", StringType(), nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("role", StringType(), nullable=True),
        StructField("seniority_level", StringType(), nullable=True),
        StructField("hourly_rate_eur", StringType(), nullable=True),
        StructField("hire_date", StringType(), nullable=True),
        StructField("last_promotion_date", StringType(), nullable=True),
        StructField("work_mode", StringType(), nullable=True),
        StructField("distance_km", StringType(), nullable=True),
        StructField("manager_id", StringType(), nullable=True),
        StructField("is_active", StringType(), nullable=True),
        StructField("termination_date", StringType(), nullable=True),
        StructField("churn_type", StringType(), nullable=True),
        StructField("snapshot_date", StringType(), nullable=True),
    ]
)


def _with_bronze_metadata(df: DataFrame, batch_date_iso: str) -> DataFrame:
    return (
        df.withColumn("_batch_date", F.to_date(F.lit(batch_date_iso)))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )


def _write_partition_idempotently(df: DataFrame, target_path: str, batch_date_iso: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_batch_date = DATE '{batch_date_iso}'")
        .partitionBy("_batch_date")
        .save(target_path)
    )


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Ingest `employees` and `timesheets` for `batch_date` into Bronze Delta tables."""
    batch_date_iso = date.fromisoformat(batch_date).isoformat()
    resolved_landing = get_landing_root(landing_root)
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)

    employees_source = join_path(
        resolved_landing, "employees", f"date={batch_date_iso}", "part-000.csv"
    )
    employees_target = join_path(resolved_lakehouse, "bronze", "employees")
    raw_employees = (
        spark.read.option("header", "true").schema(EMPLOYEES_RAW_SCHEMA).csv(employees_source)
    )
    bronze_employees = _with_bronze_metadata(raw_employees, batch_date_iso)
    _write_partition_idempotently(bronze_employees, employees_target, batch_date_iso)

    timesheets_source = join_path(
        resolved_landing, "timesheets", f"date={batch_date_iso}", "part-000.parquet"
    )
    timesheets_target = join_path(resolved_lakehouse, "bronze", "timesheets")
    raw_timesheets = spark.read.parquet(timesheets_source)
    bronze_timesheets = _with_bronze_metadata(raw_timesheets, batch_date_iso)
    _write_partition_idempotently(bronze_timesheets, timesheets_target, batch_date_iso)

    return bronze_employees, bronze_timesheets


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest HR datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-hr")
    try:
        employees_df, timesheets_df = run(
            spark, args.batch_date, args.landing_root, args.lakehouse_root
        )
        print(
            f"Ingested HR Bronze for batch_date={args.batch_date}: "
            f"employees={employees_df.count()}, timesheets={timesheets_df.count()}"
        )
    finally:
        spark.stop()
