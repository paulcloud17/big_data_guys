"""Silver transformation & quarantine job for `silver_timesheets`.

Implements daily Bronze -> Silver cleaning, deterministic deduplication,
quarantine routing, and DQ audit logging for `timesheets`:
1. Reads `bronze/timesheets` for the requested `_batch_date` and normalizes types.
2. Deduplicates exact and near-duplicates by `ts_id` using a deterministic
   `Window.partitionBy("ts_id").orderBy(...)` rule (documented in `docs/architecture.md`):
   - Prefer rows with valid `hours` in `(0, 24]` over out-of-range/null `hours`
   - Prefer rows with non-null `project_id`
   - Prefer the lowest `hours` value (conservative labor-cost attribution)
   - Final deterministic tie-breakers: `emp_id ASC`, `project_id ASC`,
     `work_date ASC`, `_ingested_at DESC`, `_source_file ASC`
3. Routes invalid rows to `silver_quarantine/timesheets` with `dq_reasons: array<string>`
   and `_batch_date`:
   - `missing_ts_id` (`ts_id IS NULL`)
   - `missing_emp_id` (`emp_id IS NULL`)
   - `missing_work_date` (`work_date IS NULL`)
   - `missing_project_id` (`project_id IS NULL`)
   - `missing_hours` (`hours IS NULL`)
   - `non_positive_hours` (`hours <= 0`)
   - `hours_exceed_24` (`hours > 24`)
   - `post_termination_entry` (`work_date >= employee.termination_date`)
4. Writes valid and quarantined partitions idempotently by `_batch_date`.
5. Persists hard (`ERROR`) and soft (`WARN`) checks to `dq/dq_audit_results`.
"""

from __future__ import annotations

import argparse
import uuid
from datetime import date, datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, StructField, StructType

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

QUARANTINE_RATIO_WARN_THRESHOLD = 0.08

BASE_COLUMNS: tuple[str, ...] = (
    "ts_id",
    "emp_id",
    "project_id",
    "work_date",
    "hours",
    "_batch_date",
    "_ingested_at",
    "_source_file",
)

SILVER_COLUMNS: tuple[str, ...] = (*BASE_COLUMNS, "_silver_updated_at")
QUARANTINE_COLUMNS: tuple[str, ...] = (*BASE_COLUMNS, "dq_reasons")


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


def _load_employee_terminations(
    spark: SparkSession,
    resolved_lakehouse: str,
    batch_date_iso: str,
) -> DataFrame:
    """Load `(emp_id, _emp_termination_date)` from Silver SCD2, falling back to Bronze employees."""
    silver_emp_path = join_path(resolved_lakehouse, "silver", "silver_employees_scd2")
    bronze_emp_path = join_path(resolved_lakehouse, "bronze", "employees")
    batch_date_col = F.to_date(F.lit(batch_date_iso))

    if DeltaTable.isDeltaTable(spark, silver_emp_path):
        emp_df = spark.read.format("delta").load(silver_emp_path)
        return (
            emp_df.select(
                _clean_str(emp_df, "emp_id").alias("emp_id"),
                _clean_date(emp_df, "termination_date").alias("_emp_termination_date"),
            )
            .filter(F.col("emp_id").isNotNull() & F.col("_emp_termination_date").isNotNull())
            .groupBy("emp_id")
            .agg(F.min("_emp_termination_date").alias("_emp_termination_date"))
        )

    if DeltaTable.isDeltaTable(spark, bronze_emp_path):
        emp_df = spark.read.format("delta").load(bronze_emp_path)
        if "_batch_date" in emp_df.columns:
            emp_df = emp_df.filter(F.to_date(F.col("_batch_date")) <= batch_date_col)
        return (
            emp_df.select(
                _clean_str(emp_df, "emp_id").alias("emp_id"),
                _clean_date(emp_df, "termination_date").alias("_emp_termination_date"),
            )
            .filter(F.col("emp_id").isNotNull() & F.col("_emp_termination_date").isNotNull())
            .groupBy("emp_id")
            .agg(F.min("_emp_termination_date").alias("_emp_termination_date"))
        )

    empty_schema = StructType(
        [
            StructField("emp_id", StringType(), nullable=True),
            StructField("_emp_termination_date", DateType(), nullable=True),
        ]
    )
    return spark.createDataFrame([], schema=empty_schema)


def _deduplicate_timesheets(normalized_df: DataFrame) -> DataFrame:
    """Deduplicate non-null `ts_id` rows deterministically; keep null IDs for quarantine."""
    null_id_rows = normalized_df.filter(F.col("ts_id").isNull()).select(*BASE_COLUMNS)
    non_null_rows = normalized_df.filter(F.col("ts_id").isNotNull())

    valid_hours_rank = F.when(
        F.col("hours").isNotNull() & (F.col("hours") > 0) & (F.col("hours") <= 24),
        F.lit(0),
    ).otherwise(F.lit(1))
    has_project_rank = F.when(F.col("project_id").isNotNull(), F.lit(0)).otherwise(F.lit(1))

    dedup_window = Window.partitionBy("ts_id").orderBy(
        valid_hours_rank.asc(),
        has_project_rank.asc(),
        F.col("hours").asc_nulls_last(),
        F.col("emp_id").asc_nulls_last(),
        F.col("project_id").asc_nulls_last(),
        F.col("work_date").asc_nulls_last(),
        F.col("_ingested_at").desc_nulls_last(),
        F.col("_source_file").asc_nulls_last(),
    )

    deduped_non_null = (
        non_null_rows.withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .select(*BASE_COLUMNS)
    )
    return deduped_non_null.unionByName(null_id_rows)


def _write_batch_partition_idempotently(
    spark: SparkSession,
    df: DataFrame,
    target_path: str,
    batch_date_iso: str,
) -> None:
    """Replace only `_batch_date = batch_date_iso` in a partitioned Delta table (handles empty)."""
    if DeltaTable.isDeltaTable(spark, target_path):
        DeltaTable.forPath(spark, target_path).delete(f"_batch_date = DATE '{batch_date_iso}'")
        if not df.isEmpty():
            (df.write.format("delta").mode("append").partitionBy("_batch_date").save(target_path))
    else:
        (df.write.format("delta").mode("overwrite").partitionBy("_batch_date").save(target_path))


def _build_dq_results(
    *,
    valid_df: DataFrame,
    quarantine_df: DataFrame,
    raw_count: int,
    deduped_count: int,
    run_id: str,
    batch_date: str,
) -> list[DQCheckResult]:
    parsed_date = _parse_batch_date(batch_date)
    now = datetime.now(timezone.utc)

    valid_count = valid_df.count()
    quarantined_count = quarantine_df.count()
    duplicate_count = max(raw_count - deduped_count, 0)
    zombie_count = quarantine_df.filter(
        F.array_contains(F.col("dq_reasons"), "post_termination_entry")
    ).count()

    not_null_ts = check_not_null(
        valid_df,
        "ts_id",
        run_id=run_id,
        batch_date=batch_date,
        layer="silver",
        table_name="silver_timesheets",
        severity="ERROR",
    )
    not_null_emp = check_not_null(
        valid_df,
        "emp_id",
        run_id=run_id,
        batch_date=batch_date,
        layer="silver",
        table_name="silver_timesheets",
        severity="ERROR",
    )

    dup_valid_ids = valid_df.groupBy("ts_id").count().filter(F.col("count") > 1).count()
    unique_ts_res = DQCheckResult(
        run_id=run_id,
        batch_date=parsed_date,
        layer="silver",
        table_name="silver_timesheets",
        check_name="unique_ts_id",
        severity="ERROR",
        status="PASS" if dup_valid_ids == 0 else "FAIL",
        failed_count=dup_valid_ids,
        total_count=valid_count,
        checked_at=now,
    )

    non_empty_res = DQCheckResult(
        run_id=run_id,
        batch_date=parsed_date,
        layer="silver",
        table_name="silver_timesheets",
        check_name="non_empty_partition",
        severity="ERROR",
        status="PASS" if raw_count > 0 else "FAIL",
        failed_count=0 if raw_count > 0 else 1,
        total_count=raw_count,
        checked_at=now,
    )

    quarantine_ratio = (quarantined_count / deduped_count) if deduped_count > 0 else 0.0
    quarantine_ratio_res = DQCheckResult(
        run_id=run_id,
        batch_date=parsed_date,
        layer="silver",
        table_name="silver_timesheets",
        check_name="quarantine_ratio",
        severity="WARN",
        status="WARN" if quarantine_ratio > QUARANTINE_RATIO_WARN_THRESHOLD else "PASS",
        failed_count=quarantined_count,
        total_count=deduped_count,
        checked_at=now,
    )

    duplicate_rate_res = DQCheckResult(
        run_id=run_id,
        batch_date=parsed_date,
        layer="silver",
        table_name="silver_timesheets",
        check_name="duplicate_rate",
        severity="WARN",
        status="WARN" if duplicate_count > 0 else "PASS",
        failed_count=duplicate_count,
        total_count=raw_count,
        checked_at=now,
    )

    zombie_res = DQCheckResult(
        run_id=run_id,
        batch_date=parsed_date,
        layer="silver",
        table_name="silver_timesheets",
        check_name="zombie_timesheet_count",
        severity="WARN",
        status="WARN" if zombie_count > 0 else "PASS",
        failed_count=zombie_count,
        total_count=deduped_count,
        checked_at=now,
    )

    return [
        not_null_ts,
        not_null_emp,
        unique_ts_res,
        non_empty_res,
        quarantine_ratio_res,
        duplicate_rate_res,
        zombie_res,
    ]


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Transform `bronze/timesheets` for `batch_date` into Silver and Quarantine Delta tables."""
    batch_date_iso = date.fromisoformat(batch_date).isoformat()
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"manual-{batch_date_iso}-{uuid.uuid4().hex[:8]}"

    bronze_path = join_path(resolved_lakehouse, "bronze", "timesheets")
    silver_path = join_path(resolved_lakehouse, "silver", "silver_timesheets")
    quarantine_path = join_path(resolved_lakehouse, "silver_quarantine", "timesheets")
    audit_path = join_path(resolved_lakehouse, "dq", "dq_audit_results")

    batch_date_col = F.to_date(F.lit(batch_date_iso))

    bronze_df = spark.read.format("delta").load(bronze_path)
    batch_df = bronze_df.filter(F.to_date(F.col("_batch_date")) == batch_date_col)

    normalized_df = batch_df.select(
        _clean_str(batch_df, "ts_id").alias("ts_id"),
        _clean_str(batch_df, "emp_id").alias("emp_id"),
        _clean_str(batch_df, "project_id").alias("project_id"),
        _clean_date(batch_df, "work_date").alias("work_date"),
        _clean_double(batch_df, "hours").alias("hours"),
        batch_date_col.alias("_batch_date"),
        (
            F.col("_ingested_at").cast("timestamp")
            if "_ingested_at" in batch_df.columns
            else F.lit(None).cast("timestamp")
        ).alias("_ingested_at"),
        (
            _clean_str(batch_df, "_source_file")
            if "_source_file" in batch_df.columns
            else F.lit(None).cast("string")
        ).alias("_source_file"),
    ).localCheckpoint(eager=True)

    raw_count = normalized_df.count()
    deduped_df = _deduplicate_timesheets(normalized_df).localCheckpoint(eager=True)
    deduped_count = deduped_df.count()

    terminations_df = _load_employee_terminations(spark, resolved_lakehouse, batch_date_iso)
    enriched_df = deduped_df.join(terminations_df, on="emp_id", how="left")

    reason_exprs = F.array(
        F.when(F.col("ts_id").isNull(), F.lit("missing_ts_id")),
        F.when(F.col("emp_id").isNull(), F.lit("missing_emp_id")),
        F.when(F.col("work_date").isNull(), F.lit("missing_work_date")),
        F.when(F.col("project_id").isNull(), F.lit("missing_project_id")),
        F.when(F.col("hours").isNull(), F.lit("missing_hours")),
        F.when(F.col("hours").isNotNull() & (F.col("hours") <= 0), F.lit("non_positive_hours")),
        F.when(F.col("hours").isNotNull() & (F.col("hours") > 24), F.lit("hours_exceed_24")),
        F.when(
            F.col("_emp_termination_date").isNotNull()
            & F.col("work_date").isNotNull()
            & (F.col("work_date") >= F.col("_emp_termination_date")),
            F.lit("post_termination_entry"),
        ),
    )

    evaluated_df = enriched_df.withColumn(
        "dq_reasons", F.filter(reason_exprs, lambda reason: reason.isNotNull())
    ).localCheckpoint(eager=True)

    quarantine_df = evaluated_df.filter(F.size("dq_reasons") > 0).select(*QUARANTINE_COLUMNS)
    valid_df = (
        evaluated_df.filter(F.size("dq_reasons") == 0)
        .withColumn("_silver_updated_at", F.current_timestamp())
        .select(*SILVER_COLUMNS)
    )

    _write_batch_partition_idempotently(spark, valid_df, silver_path, batch_date_iso)
    _write_batch_partition_idempotently(spark, quarantine_df, quarantine_path, batch_date_iso)

    dq_results = _build_dq_results(
        valid_df=valid_df,
        quarantine_df=quarantine_df,
        raw_count=raw_count,
        deduped_count=deduped_count,
        run_id=resolved_run_id,
        batch_date=batch_date_iso,
    )
    write_audit_results(spark, dq_results, audit_path)
    enforce_dq_gate(dq_results)

    return valid_df, quarantine_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform bronze/timesheets into silver_timesheets."
    )
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-timesheets")
    try:
        valid_df, quarantine_df = run(spark, args.batch_date, args.lakehouse_root, args.run_id)
        print(
            f"silver_timesheets complete for batch_date={args.batch_date}: "
            f"valid={valid_df.count()}, quarantined={quarantine_df.count()}"
        )
    finally:
        spark.stop()
