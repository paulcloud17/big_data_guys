"""Silver SCD Type 2 transformation for `silver_employees_scd2`.

Implements incremental SCD Type 2 history over daily Bronze employee snapshots:
- Reads `bronze/employees` filtered to the requested `_batch_date`
- Tracks changes across all 6 business attributes (`department`, `role`,
  `hourly_rate_eur`, `is_active`, `termination_date`, `churn_type`), using
  NULL-safe equality so `NULL <-> non-NULL` transitions create new versions
- Uses half-open validity intervals `[valid_from, valid_to)` where `valid_from`
  is inclusive, `valid_to` is exclusive, and current rows have `valid_to = NULL`
  and `is_current = True` (see `docs/architecture.md`)
- Guarantees idempotency when re-running the same `batch_date`
- Validates uniqueness of `(emp_id, valid_from)` and zero overlapping intervals
  and persists results to `dq/dq_audit_results`
"""

from __future__ import annotations

import argparse
import operator
import uuid
from datetime import datetime, timezone
from functools import reduce

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType

from dq.framework import (
    DQCheckResult,
    _parse_batch_date,
    check_not_null,
    enforce_dq_gate,
    write_audit_results,
)
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_spark_session,
    join_path,
)

TRACKED_COLUMNS: tuple[str, ...] = (
    "department",
    "role",
    "hourly_rate_eur",
    "is_active",
    "termination_date",
    "churn_type",
)

EMPLOYEE_COLUMNS: tuple[str, ...] = (
    "emp_id",
    "full_name",
    "department",
    "role",
    "seniority_level",
    "hourly_rate_eur",
    "hire_date",
    "last_promotion_date",
    "work_mode",
    "distance_km",
    "manager_id",
    "is_active",
    "termination_date",
    "churn_type",
    "snapshot_date",
)

SCD2_COLUMNS: tuple[str, ...] = (
    *EMPLOYEE_COLUMNS,
    "valid_from",
    "valid_to",
    "is_current",
    "_silver_updated_at",
)


def _clean_str(df: DataFrame, column_name: str) -> Column:
    col_expr = F.col(column_name)
    if not isinstance(df.schema[column_name].dataType, StringType):
        col_expr = col_expr.cast("string")
    trimmed = F.trim(col_expr)
    return F.when(F.length(trimmed) == 0, F.lit(None).cast("string")).otherwise(trimmed)


def _clean_date(df: DataFrame, column_name: str) -> Column:
    if isinstance(df.schema[column_name].dataType, DateType):
        return F.col(column_name)
    return F.to_date(_clean_str(df, column_name))


def _clean_double(df: DataFrame, column_name: str) -> Column:
    if isinstance(df.schema[column_name].dataType, StringType):
        return _clean_str(df, column_name).cast("double")
    return F.col(column_name).cast("double")


def _clean_bool(df: DataFrame, column_name: str) -> Column:
    if isinstance(df.schema[column_name].dataType, StringType):
        lowered = F.lower(_clean_str(df, column_name))
        return (
            F.when(lowered == "true", F.lit(True))
            .when(lowered == "false", F.lit(False))
            .otherwise(F.lit(None).cast("boolean"))
        )
    return F.col(column_name).cast("boolean")


def _tracked_fields_differ(left_alias: str, right_alias: str) -> Column:
    """Return a boolean Column that is True iff any of the 6 tracked fields differs (NULL-safe)."""
    return reduce(
        operator.or_,
        [
            ~F.col(f"{left_alias}.{col}").eqNullSafe(F.col(f"{right_alias}.{col}"))
            for col in TRACKED_COLUMNS
        ],
    )


def _prepare_bronze_snapshot(bronze_df: DataFrame, batch_date: str) -> DataFrame:
    """Filter Bronze employees to `batch_date`, normalize types, and deduplicate by `emp_id`."""
    batch_date_col = F.to_date(F.lit(batch_date))
    snapshot_df = bronze_df.filter(F.to_date(F.col("_batch_date")) == batch_date_col)

    normalized_df = snapshot_df.select(
        _clean_str(snapshot_df, "emp_id").alias("emp_id"),
        _clean_str(snapshot_df, "full_name").alias("full_name"),
        _clean_str(snapshot_df, "department").alias("department"),
        _clean_str(snapshot_df, "role").alias("role"),
        _clean_str(snapshot_df, "seniority_level").alias("seniority_level"),
        _clean_double(snapshot_df, "hourly_rate_eur").alias("hourly_rate_eur"),
        _clean_date(snapshot_df, "hire_date").alias("hire_date"),
        _clean_date(snapshot_df, "last_promotion_date").alias("last_promotion_date"),
        _clean_str(snapshot_df, "work_mode").alias("work_mode"),
        _clean_double(snapshot_df, "distance_km").alias("distance_km"),
        _clean_str(snapshot_df, "manager_id").alias("manager_id"),
        _clean_bool(snapshot_df, "is_active").alias("is_active"),
        _clean_date(snapshot_df, "termination_date").alias("termination_date"),
        _clean_str(snapshot_df, "churn_type").alias("churn_type"),
        F.coalesce(_clean_date(snapshot_df, "snapshot_date"), batch_date_col).alias(
            "snapshot_date"
        ),
        *(
            [F.col("_ingested_at")]
            if "_ingested_at" in snapshot_df.columns
            else [F.lit(None).cast("timestamp").alias("_ingested_at")]
        ),
    ).filter(F.col("emp_id").isNotNull())

    dedup_window = Window.partitionBy("emp_id").orderBy(
        F.col("_ingested_at").desc_nulls_last(),
        F.col("full_name").asc_nulls_last(),
    )
    return (
        normalized_df.withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .select(*EMPLOYEE_COLUMNS)
    )


def _open_new_versions(df: DataFrame, batch_date_col: Column, updated_at: Column) -> DataFrame:
    return (
        df.withColumn("valid_from", batch_date_col)
        .withColumn("valid_to", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn("_silver_updated_at", updated_at)
        .select(*SCD2_COLUMNS)
    )


def _build_scd2_dq_results(
    scd2_df: DataFrame,
    *,
    run_id: str,
    batch_date: str,
) -> list[DQCheckResult]:
    parsed_date = _parse_batch_date(batch_date)
    now = datetime.now(timezone.utc)
    total_count = scd2_df.count()

    not_null_emp = check_not_null(
        scd2_df,
        "emp_id",
        run_id=run_id,
        batch_date=batch_date,
        layer="silver",
        table_name="silver_employees_scd2",
        severity="ERROR",
    )

    dup_keys = scd2_df.groupBy("emp_id", "valid_from").count().filter(F.col("count") > 1).count()
    unique_key_res = DQCheckResult(
        run_id=run_id,
        batch_date=parsed_date,
        layer="silver",
        table_name="silver_employees_scd2",
        check_name="unique_emp_id_valid_from",
        severity="ERROR",
        status="PASS" if dup_keys == 0 else "FAIL",
        failed_count=dup_keys,
        total_count=total_count,
        checked_at=now,
    )

    interval_window = Window.partitionBy("emp_id").orderBy("valid_from")
    invalid_intervals = (
        scd2_df.withColumn("_next_valid_from", F.lead("valid_from").over(interval_window))
        .filter(
            F.col("valid_from").isNull()
            | (F.col("valid_to").isNotNull() & (F.col("valid_to") <= F.col("valid_from")))
            | (
                F.col("_next_valid_from").isNotNull()
                & (F.col("valid_to").isNull() | (F.col("valid_to") > F.col("_next_valid_from")))
            )
            | (F.col("is_current") & F.col("valid_to").isNotNull())
            | (~F.col("is_current") & F.col("valid_to").isNull())
        )
        .count()
    )
    no_overlap_res = DQCheckResult(
        run_id=run_id,
        batch_date=parsed_date,
        layer="silver",
        table_name="silver_employees_scd2",
        check_name="scd2_no_overlaps",
        severity="ERROR",
        status="PASS" if invalid_intervals == 0 else "FAIL",
        failed_count=invalid_intervals,
        total_count=total_count,
        checked_at=now,
    )

    return [not_null_emp, unique_key_res, no_overlap_res]


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> DataFrame:
    """Build or incrementally update `silver/silver_employees_scd2` for `batch_date`."""
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"manual-{batch_date}-{uuid.uuid4().hex[:8]}"

    bronze_path = join_path(resolved_lakehouse, "bronze", "employees")
    silver_path = join_path(resolved_lakehouse, "silver", "silver_employees_scd2")
    audit_path = join_path(resolved_lakehouse, "dq", "dq_audit_results")

    batch_date_col = F.to_date(F.lit(batch_date))
    now_ts = F.current_timestamp()

    bronze_df = spark.read.format("delta").load(bronze_path)
    incoming_df = _prepare_bronze_snapshot(bronze_df, batch_date).localCheckpoint(eager=True)

    if not DeltaTable.isDeltaTable(spark, silver_path):
        final_df = _open_new_versions(incoming_df, batch_date_col, now_ts)
        (
            final_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(silver_path)
        )
    else:
        existing_df = (
            spark.read.format("delta")
            .load(silver_path)
            .select(*SCD2_COLUMNS)
            .localCheckpoint(eager=True)
        )

        # Find the version covering `batch_date` under half-open [valid_from, valid_to).
        covering_df = existing_df.filter(
            (F.col("valid_from") <= batch_date_col)
            & (F.col("valid_to").isNull() | (batch_date_col < F.col("valid_to")))
        )

        compared_df = incoming_df.alias("inc").join(
            covering_df.alias("cov"),
            on=F.col("inc.emp_id") == F.col("cov.emp_id"),
            how="left",
        )
        affected_incoming_df = compared_df.filter(
            F.col("cov.emp_id").isNull() | _tracked_fields_differ("cov", "inc")
        ).select([F.col(f"inc.{col}") for col in EMPLOYEE_COLUMNS])

        if affected_incoming_df.isEmpty():
            final_df = existing_df
        else:
            affected_ids = affected_incoming_df.select("emp_id").distinct()
            untouched_existing = existing_df.join(affected_ids, on="emp_id", how="left_anti")
            affected_existing = existing_df.join(affected_ids, on="emp_id", how="left_semi")

            # Guard against out-of-order historical mutation when newer versions already exist.
            future_rows = affected_existing.filter(F.col("valid_from") > batch_date_col)
            if not future_rows.isEmpty():
                raise ValueError(
                    f"Cannot mutate silver_employees_scd2 for batch_date={batch_date} when "
                    "versions with valid_from > batch_date already exist."
                )

            # Rewind any prior run of `batch_date` for affected employees so re-runs stay
            # strictly idempotent even if Bronze data for `batch_date` was corrected.
            rewound_existing = (
                affected_existing.filter(F.col("valid_from") != batch_date_col)
                .withColumn(
                    "is_current",
                    F.when(F.col("valid_to") == batch_date_col, F.lit(True)).otherwise(
                        F.col("is_current")
                    ),
                )
                .withColumn(
                    "valid_to",
                    F.when(F.col("valid_to") == batch_date_col, F.lit(None).cast("date")).otherwise(
                        F.col("valid_to")
                    ),
                )
                .select(*SCD2_COLUMNS)
            )

            historical_closed = rewound_existing.filter(~F.col("is_current"))
            pre_d_open = rewound_existing.filter(F.col("is_current"))

            open_joined = pre_d_open.alias("cur").join(
                affected_incoming_df.alias("inc"),
                on=F.col("cur.emp_id") == F.col("inc.emp_id"),
                how="inner",
            )
            diff_expr = _tracked_fields_differ("cur", "inc")

            # If corrected Bronze data now matches the pre-D open row, keep it open unchanged.
            reopened_unchanged = open_joined.filter(~diff_expr).select(
                [F.col(f"cur.{col}") for col in SCD2_COLUMNS]
            )

            # Close pre-D open rows whose tracked attributes changed on `batch_date`.
            newly_closed = (
                open_joined.filter(diff_expr)
                .select([F.col(f"cur.{col}") for col in SCD2_COLUMNS])
                .withColumn("valid_to", batch_date_col)
                .withColumn("is_current", F.lit(False))
                .withColumn("_silver_updated_at", now_ts)
                .select(*SCD2_COLUMNS)
            )

            # Open new versions for changed employees and brand-new employees.
            changed_incoming = open_joined.filter(diff_expr).select(
                [F.col(f"inc.{col}") for col in EMPLOYEE_COLUMNS]
            )
            brand_new_incoming = affected_incoming_df.join(
                pre_d_open.select("emp_id"), on="emp_id", how="left_anti"
            )
            newly_opened = _open_new_versions(
                changed_incoming.unionByName(brand_new_incoming),
                batch_date_col,
                now_ts,
            )

            final_df = (
                untouched_existing.unionByName(historical_closed)
                .unionByName(reopened_unchanged)
                .unionByName(newly_closed)
                .unionByName(newly_opened)
            )

            (
                final_df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(silver_path)
            )

    persisted_df = spark.read.format("delta").load(silver_path)
    dq_results = _build_scd2_dq_results(
        persisted_df,
        run_id=resolved_run_id,
        batch_date=batch_date,
    )
    write_audit_results(spark, dq_results, audit_path)
    enforce_dq_gate(dq_results)

    return persisted_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver_employees_scd2.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-employees-scd2")
    try:
        df = run(spark, args.batch_date, args.lakehouse_root, args.run_id)
        print(f"silver_employees_scd2 complete for batch_date={args.batch_date}: rows={df.count()}")
    finally:
        spark.stop()
