"""Silver FX rates: calendar spine + LOCF forward-fill -> `silver/silver_fx_rates_daily`.

Why a calendar spine?
    FX files arrive on weekdays only, but invoices/expenses happen every day. If we just
    joined on `rate_date`, weekend invoices would get NULL rates. So we first build the
    full grid "every calendar day x every currency pair" (the *spine*) and left-join the
    real rates onto it. Weekend rows exist but have `rate = NULL`.

Why LOCF (Last Observation Carried Forward)?
    For each currency pair, ordered by date, `last(rate, ignorenulls=True)` over the
    window "all rows up to and including the current one" returns the most recent
    non-NULL rate -> Saturday and Sunday inherit Friday's rate.

Design decisions:
    - Full recompute on every run from ALL Bronze FX rows (spine from the first FX date to
      the furthest of `batch_date` and the current Silver end date). The table is tiny
      (~6 rows/day), so a full rebuild is cheap, idempotent (same input -> same output) and
      backfill-safe: re-running an older `batch_date` neither truncates later days nor
      replaces their observed rates, and a late-arriving Friday file re-fills its weekend.
    - The spine always contains the fixed set of 6 pairs (all ordered pairs of EUR/USD/CZK).
      A pair with no valid observation therefore shows up as NULL rates and fails the hard
      `not_null_rate` check instead of silently disappearing.
    - DQ checks run BEFORE writing: if a hard check fails, the previous Silver table stays
      untouched (only the audit rows are written).
    - Invalid raw FX rows (NULL/<=0 rate, unknown currency, unparseable date) go to
      `silver_quarantine/fx_rates` with `dq_reasons`; the gap they leave is forward-filled.
    - Duplicate raw rows for the same (rate_date, pair): keep the latest `_batch_date`,
      then latest `_ingested_at`, then the first row in the file (`_source_row_id`), then the
      lowest rate -> fully deterministic.
    - Extra columns for traceability: `is_forward_filled`, `source_rate_date`.

Sanity check (Days 1-14, 2026-01-01..2026-01-14): 14 days x 6 pairs = 84 rows, 0 NULL rates.
"""

from __future__ import annotations

import argparse
import uuid
from datetime import date, datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.framework import (
    DQCheckResult,
    check_not_null,
    enforce_dq_gate,
    write_audit_results,
)
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_spark_session,
    join_path,
)

VALID_CURRENCIES = ("EUR", "USD", "CZK")
EXPECTED_PAIRS = tuple((a, b) for a in VALID_CURRENCIES for b in VALID_CURRENCIES if a != b)
PAIR_COLS = ["currency_from", "currency_to"]
SILVER_TABLE = "silver_fx_rates_daily"


def _clean_currency(column_name: str) -> Column:
    trimmed = F.upper(F.trim(F.col(column_name)))
    return F.when(F.length(trimmed) == 0, F.lit(None).cast("string")).otherwise(trimmed)


def _parse_date(column_name: str) -> Column:
    """Accept ISO `YYYY-MM-DD` and European `DD.MM.YYYY` (returns NULL if neither)."""
    raw = F.trim(F.col(column_name))
    return F.coalesce(F.to_date(raw, "yyyy-MM-dd"), F.to_date(raw, "dd.MM.yyyy"))


def _evaluate_bronze(bronze_df: DataFrame) -> DataFrame:
    """Normalize raw FX rows and attach `dq_reasons` (empty array = valid)."""
    normalized = (
        bronze_df.withColumn("currency_from", _clean_currency("currency_from"))
        .withColumn("currency_to", _clean_currency("currency_to"))
        .withColumn("rate_date_parsed", _parse_date("rate_date"))
    )
    reasons = F.array(
        F.when(F.col("rate_date_parsed").isNull(), F.lit("invalid_rate_date")),
        F.when(
            F.col("currency_from").isNull() | ~F.col("currency_from").isin(*VALID_CURRENCIES),
            F.lit("invalid_currency_from"),
        ),
        F.when(
            F.col("currency_to").isNull() | ~F.col("currency_to").isin(*VALID_CURRENCIES),
            F.lit("invalid_currency_to"),
        ),
        F.when(F.col("currency_from") == F.col("currency_to"), F.lit("same_currency_pair")),
        F.when(F.col("rate").isNull() | (F.col("rate") <= 0), F.lit("invalid_rate")),
    )
    return normalized.withColumn("dq_reasons", F.filter(reasons, lambda r: r.isNotNull()))


def _resolve_end_date(spark: SparkSession, silver_path: str, batch_date: str) -> date:
    """Spine end = max(batch_date, current Silver end) so backfills never shrink the table."""
    end = date.fromisoformat(batch_date)
    if DeltaTable.isDeltaTable(spark, silver_path):
        existing_max = (
            spark.read.format("delta").load(silver_path).agg(F.max("rate_date")).first()[0]
        )
        if existing_max is not None and existing_max > end:
            end = existing_max
    return end


def build_fx_rates_daily(valid_rates: DataFrame, end_date: date) -> DataFrame:
    """Pure transformation: spine x pairs, left-join observed rates, LOCF forward-fill.

    `valid_rates` columns: rate_date (date), currency_from, currency_to, rate (one row per
    date & pair). Returns one row per (rate_date, pair) from the first observed date to
    `end_date`, including `is_forward_filled` and `source_rate_date`.
    """
    start_date = valid_rates.agg(F.min("rate_date")).first()[0]
    if start_date is None:
        raise ValueError("No valid FX rates in Bronze - cannot build the calendar spine.")

    spark = valid_rates.sparkSession
    # 1) Calendar spine: one row per day from start_date to end_date (inclusive).
    spine = spark.range(1).select(
        F.explode(F.sequence(F.lit(start_date), F.lit(end_date), F.expr("INTERVAL 1 DAY"))).alias(
            "rate_date"
        )
    )
    # 2) Every day x every expected currency pair (small x small cross join -> safe).
    pairs = spark.createDataFrame(list(EXPECTED_PAIRS), PAIR_COLS)
    grid = spine.crossJoin(pairs)

    # 3) Attach observed rates; weekends/holidays stay NULL for now.
    joined = grid.join(valid_rates, on=["rate_date", *PAIR_COLS], how="left").withColumn(
        "observed_date", F.when(F.col("rate").isNotNull(), F.col("rate_date"))
    )

    # 4) LOCF: per pair, ordered by date, take the last non-NULL value seen so far.
    locf_window = (
        Window.partitionBy(*PAIR_COLS)
        .orderBy("rate_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return joined.select(
        "rate_date",
        *PAIR_COLS,
        F.last("rate", ignorenulls=True).over(locf_window).alias("rate"),
        F.col("rate").isNull().alias("is_forward_filled"),
        F.last("observed_date", ignorenulls=True).over(locf_window).alias("source_rate_date"),
    )


def _completeness_check(fx_daily: DataFrame, run_id: str, batch_date: str) -> DQCheckResult:
    """ERROR check: exactly one row per (rate_date, expected pair) over the whole spine."""
    stats = fx_daily.agg(
        F.count(F.lit(1)).alias("total"),
        F.countDistinct("rate_date").alias("days"),
    ).first()
    expected = stats["days"] * len(EXPECTED_PAIRS)
    distinct_keys = fx_daily.select("rate_date", *PAIR_COLS).distinct().count()
    failed = abs(stats["total"] - expected) + (stats["total"] - distinct_keys)
    return DQCheckResult(
        run_id=run_id,
        batch_date=date.fromisoformat(batch_date),
        layer="silver",
        table_name=SILVER_TABLE,
        check_name="complete_unique_spine",
        severity="ERROR",
        status="PASS" if failed == 0 else "FAIL",
        failed_count=int(failed),
        total_count=int(stats["total"]),
        checked_at=datetime.now(timezone.utc),
    )


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Build `silver/silver_fx_rates_daily` (spine + LOCF) and `silver_quarantine/fx_rates`."""
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"manual-{batch_date}-{uuid.uuid4().hex[:8]}"

    bronze_path = join_path(resolved_lakehouse, "bronze", "fx_rates")
    silver_path = join_path(resolved_lakehouse, "silver", SILVER_TABLE)
    quarantine_path = join_path(resolved_lakehouse, "silver_quarantine", "fx_rates")
    audit_path = join_path(resolved_lakehouse, "dq", "dq_audit_results")

    # Full rebuild from ALL Bronze rows (not only <= batch_date): a backfill of an older day
    # must not replace later observed rates with stale forward-filled values.
    bronze_df = spark.read.format("delta").load(bronze_path)
    evaluated = _evaluate_bronze(bronze_df)

    quarantine_df = evaluated.filter(F.size("dq_reasons") > 0).drop("rate_date_parsed")

    dedup_window = Window.partitionBy("rate_date", *PAIR_COLS).orderBy(
        F.col("_batch_date").desc(),
        F.col("_ingested_at").desc(),
        F.col("_source_row_id").asc(),
        F.col("rate").asc(),
    )
    valid_rates = (
        evaluated.filter(F.size("dq_reasons") == 0)
        .select(
            F.col("rate_date_parsed").alias("rate_date"),
            *PAIR_COLS,
            "rate",
            "_batch_date",
            "_ingested_at",
            "_source_row_id",
        )
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .select("rate_date", *PAIR_COLS, "rate")
    )

    end_date = _resolve_end_date(spark, silver_path, batch_date)
    fx_daily = build_fx_rates_daily(valid_rates, end_date).withColumn(
        "_silver_updated_at", F.current_timestamp()
    )
    # Compute once, so the write and the DQ checks below see exactly the same rows.
    fx_daily = fx_daily.localCheckpoint(eager=True)

    dq_results = [
        check_not_null(
            fx_daily,
            "rate",
            run_id=resolved_run_id,
            batch_date=batch_date,
            layer="silver",
            table_name=SILVER_TABLE,
            severity="ERROR",
        ),
        _completeness_check(fx_daily, resolved_run_id, batch_date),
    ]
    write_audit_results(spark, dq_results, audit_path)
    enforce_dq_gate(dq_results)  # raises BEFORE anything is overwritten

    # Full rebuild -> idempotent overwrite of the whole (small) table.
    (
        fx_daily.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )
    # Quarantine is also derived from all Bronze rows -> full overwrite.
    (
        quarantine_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("_batch_date")
        .save(quarantine_path)
    )
    return fx_daily, quarantine_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver_fx_rates_daily.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-fx-rates")
    try:
        fx_df, q_df = run(spark, args.batch_date, args.lakehouse_root, args.run_id)
        print(
            f"silver_fx_rates_daily rebuilt for batch_date={args.batch_date}: "
            f"rows={fx_df.count()}, quarantined_raw_rows={q_df.count()}"
        )
    finally:
        spark.stop()
