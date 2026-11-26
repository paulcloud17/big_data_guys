"""Silver transformation & quarantine for the `projects` dimension -> `silver/silver_projects`.

Same pattern as the reference `transform_clients.py` (full-load dimension):
- trim/normalize strings, validate `status` domain and `budget_eur >= 0`,
- quarantine invalid rows to `silver_quarantine/projects` with `dq_reasons`,
- deterministic dedup by `project_id`, full overwrite (idempotent),
- hard DQ check `not_null_project_id` persisted to `dq_audit_results`.

Used by `silver_invoices` for the referential-integrity (`left_anti`) orphan check, and later
by Gold (`budget_utilization_pct`).
"""

from __future__ import annotations

import argparse
import uuid

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.framework import check_not_null, enforce_dq_gate, write_audit_results
from spark_jobs.common.spark_session import get_lakehouse_root, get_spark_session, join_path

VALID_PROJECT_STATUSES = ("Planned", "Active", "Completed", "OnHold")


def _clean_str(column_name: str) -> Column:
    trimmed = F.trim(F.col(column_name))
    return F.when(F.length(trimmed) == 0, F.lit(None).cast("string")).otherwise(trimmed)


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Build `silver/silver_projects` and `silver_quarantine/projects`."""
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"manual-{batch_date}-{uuid.uuid4().hex[:8]}"

    bronze_path = join_path(resolved_lakehouse, "bronze", "projects")
    silver_path = join_path(resolved_lakehouse, "silver", "silver_projects")
    quarantine_path = join_path(resolved_lakehouse, "silver_quarantine", "projects")
    audit_path = join_path(resolved_lakehouse, "dq", "dq_audit_results")

    normalized = (
        spark.read.format("delta")
        .load(bronze_path)
        .withColumn("project_id", _clean_str("project_id"))
        .withColumn("client_id", _clean_str("client_id"))
        .withColumn("status", _clean_str("status"))
        .withColumn("_batch_date", F.to_date(F.lit(batch_date)))
    )
    reasons = F.array(
        F.when(F.col("project_id").isNull(), F.lit("missing_project_id")),
        F.when(F.col("client_id").isNull(), F.lit("missing_client_id")),
        F.when(
            F.col("status").isNull() | ~F.col("status").isin(*VALID_PROJECT_STATUSES),
            F.lit("invalid_project_status"),
        ),
        F.when(
            F.col("budget_eur").isNull() | (F.col("budget_eur") < 0), F.lit("invalid_budget_eur")
        ),
    )
    evaluated = normalized.withColumn("dq_reasons", F.filter(reasons, lambda r: r.isNotNull()))
    quarantine_df = evaluated.filter(F.size("dq_reasons") > 0)

    dedup_window = Window.partitionBy("project_id").orderBy(
        F.col("_ingested_at").desc(), F.col("client_id").asc()
    )
    valid_df = (
        evaluated.filter(F.size("dq_reasons") == 0)
        .drop("dq_reasons")
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("_silver_updated_at", F.current_timestamp())
    )

    valid_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        silver_path
    )

    if DeltaTable.isDeltaTable(spark, quarantine_path):
        (
            quarantine_df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"_batch_date = DATE '{batch_date}'")
            .save(quarantine_path)
        )
    else:
        quarantine_df.write.format("delta").mode("overwrite").partitionBy("_batch_date").save(
            quarantine_path
        )

    dq_result = check_not_null(
        valid_df,
        "project_id",
        run_id=resolved_run_id,
        batch_date=batch_date,
        layer="silver",
        table_name="silver_projects",
        severity="ERROR",
    )
    write_audit_results(spark, [dq_result], audit_path)
    enforce_dq_gate([dq_result])
    return valid_df, quarantine_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform bronze/projects into silver_projects.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-projects")
    try:
        valid, quarantined = run(spark, args.batch_date, args.lakehouse_root, args.run_id)
        print(f"silver_projects: valid={valid.count()}, quarantined={quarantined.count()}")
    finally:
        spark.stop()
