"""Silver Finance facts: `silver/silver_invoices` and `silver/silver_expenses`.

Both jobs are **incremental per `batch_date`**: they read only that day's Bronze partition and
atomically replace that day's Silver + quarantine partitions (`replaceWhere`), so re-runs and
backfills are idempotent. Run order per day: Bronze -> `silver_projects` & `silver_fx_rates_daily`
-> this job.

silver_invoices
    1. Parse `issued_at` / `paid_at` from ISO `YYYY-MM-DD` *or* European `DD.MM.YYYY`:
       `coalesce(to_date(x, 'yyyy-MM-dd'), to_date(x, 'dd.MM.yyyy'))` - the first format that
       parses wins, an unparseable value stays NULL and is quarantined.
    2. Referential integrity: `left_anti` join against `silver_projects` returns invoices
       whose `project_id` has NO match (orphans such as `PRJ_ORPHAN_*`) -> quarantined with
       `orphan_project_id`; `left_semi` returns the matching ones. Projects are broadcast
       (800 rows) so this is a cheap map-side join without shuffling the invoices.
    3. Negative `amount` = **credit note** -> kept as valid (`is_credit_note = true`);
       `paid_date IS NULL` = open invoice -> kept as valid (`is_paid = false`).
    4. EUR conversion: rate of `currency -> EUR` on `issued_date` from `silver_fx_rates_daily`
       (weekends already forward-filled there); EUR rows use rate 1.0.
       `amount_eur` keeps the sign, so `net_amount_eur` (= signed `amount_eur`) can simply be
       summed: net revenue = invoices + credit notes.

silver_expenses
    1. Dedup by `exp_id`. Duplicates (exact copies and near-duplicates with a different
       `amount`) only occur within the same daily file. Tie-breaker: keep the **first
       occurrence in the file** (`_source_row_id` ascending), i.e. the original record;
       later rows are re-sends. Removed duplicates are counted in `dq_audit_results`
       (`duplicate_rate`) so nothing disappears silently.
    2. Validate domain values; quarantine invalid rows with `dq_reasons`.
    3. `amount_eur` via `silver_fx_rates_daily` on `expense_date`.

Hard DQ (ERROR): zero NULL EUR amounts, non-null + unique business keys per batch.
Soft DQ (WARN): quarantine ratio > 8%, orphan invoice rate > 2%, expense duplicate rate > 10%.
"""

from __future__ import annotations

import argparse
import uuid

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from dq.checks import check_quarantine_ratio, check_unique_key, check_zero_null_eur
from dq.framework import DQCheckResult, check_not_null, enforce_dq_gate, write_audit_results
from spark_jobs.common.delta_io import replace_batch_partition
from spark_jobs.common.spark_session import get_lakehouse_root, get_spark_session, join_path

VALID_CURRENCIES = ("EUR", "USD", "CZK")
VALID_DEPARTMENTS = ("Engineering", "Data", "Consulting", "Sales", "Finance", "Operations")
VALID_EXPENSE_CATEGORIES = (
    "Cloud Infrastructure",
    "Software Licenses",
    "Travel & Lodging",
    "Training",
    "Hardware",
    "Office & Events",
    "Subcontractors",
)
MAX_QUARANTINE_RATIO = 0.08
MAX_ORPHAN_RATIO = 0.02
MAX_DUPLICATE_RATIO = 0.10


# ----------------------------------------------------------------------------- helpers


def _clean_str(column_name: str) -> Column:
    trimmed = F.trim(F.col(column_name))
    return F.when(F.length(trimmed) == 0, F.lit(None).cast("string")).otherwise(trimmed)


def parse_mixed_date(column_name: str) -> Column:
    """ISO `YYYY-MM-DD` or European `DD.MM.YYYY` -> DATE (NULL if neither matches)."""
    raw = F.trim(F.col(column_name))
    return F.coalesce(F.to_date(raw, "yyyy-MM-dd"), F.to_date(raw, "dd.MM.yyyy"))


def _with_reasons(df: DataFrame, *reason_exprs: Column) -> DataFrame:
    return df.withColumn("dq_reasons", F.filter(F.array(*reason_exprs), lambda r: r.isNotNull()))


def add_amount_eur(
    df: DataFrame, fx_daily: DataFrame, date_col: str, prefix: str = "fx"
) -> DataFrame:
    """Left-join the `currency -> EUR` rate on `date_col`; EUR rows use 1.0.

    A missing rate stays NULL on purpose: the hard `zero_null_amount_eur` check then fails the
    pipeline loudly (it means FX was not built for that date) instead of hiding the problem.
    """
    to_eur = F.broadcast(
        fx_daily.filter(F.col("currency_to") == "EUR").select(
            F.col("rate_date").alias(f"_{prefix}_date"),
            F.col("currency_from").alias(f"_{prefix}_currency"),
            F.col("rate").alias(f"_{prefix}_rate"),
        )
    )
    joined = df.join(
        to_eur,
        (F.col(date_col) == F.col(f"_{prefix}_date"))
        & (F.col("currency") == F.col(f"_{prefix}_currency")),
        "left",
    )
    rate = F.when(F.col("currency") == "EUR", F.lit(1.0)).otherwise(F.col(f"_{prefix}_rate"))
    return (
        joined.withColumn("fx_rate_to_eur", rate)
        .withColumn("amount_eur", F.round(F.col("amount") * F.col("fx_rate_to_eur"), 2))
        .drop(f"_{prefix}_date", f"_{prefix}_currency", f"_{prefix}_rate")
    )


def _keep_first_per_key(df: DataFrame, key: str) -> DataFrame:
    """Deterministic dedup: first occurrence in the source file wins."""
    w = Window.partitionBy(key).orderBy(F.col("_source_row_id").asc(), F.col("_ingested_at").asc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def _load_batch(spark: SparkSession, path: str, batch_date: str) -> DataFrame:
    return (
        spark.read.format("delta")
        .load(path)
        .filter(F.col("_batch_date") == F.to_date(F.lit(batch_date)))
    )


# ----------------------------------------------------------------------------- invoices


def transform_invoices(
    spark: SparkSession, batch_date: str, lakehouse_root: str, run_id: str
) -> tuple[DataFrame, DataFrame, list[DQCheckResult]]:
    bronze_path = join_path(lakehouse_root, "bronze", "invoices")
    silver_path = join_path(lakehouse_root, "silver", "silver_invoices")
    quarantine_path = join_path(lakehouse_root, "silver_quarantine", "invoices")

    normalized = (
        _load_batch(spark, bronze_path, batch_date)
        .withColumn("inv_id", _clean_str("inv_id"))
        .withColumn("client_id", _clean_str("client_id"))
        .withColumn("project_id", _clean_str("project_id"))
        .withColumn("currency", F.upper(_clean_str("currency")))
        .withColumn("issued_date", parse_mixed_date("issued_at"))
        .withColumn("paid_date", parse_mixed_date("paid_at"))
    )
    evaluated = _with_reasons(
        normalized,
        F.when(F.col("inv_id").isNull(), F.lit("missing_inv_id")),
        F.when(F.col("issued_date").isNull(), F.lit("invalid_issued_date")),
        F.when(
            F.col("paid_at").isNotNull() & F.col("paid_date").isNull(), F.lit("invalid_paid_date")
        ),
        F.when(F.col("paid_date") < F.col("issued_date"), F.lit("paid_before_issued")),
        F.when(
            F.col("currency").isNull() | ~F.col("currency").isin(*VALID_CURRENCIES),
            F.lit("invalid_currency"),
        ),
        F.when(F.col("amount").isNull(), F.lit("missing_amount")),
    )

    # Referential integrity against silver_projects (left_anti = "no matching project").
    known_projects = F.broadcast(
        spark.read.format("delta")
        .load(join_path(lakehouse_root, "silver", "silver_projects"))
        .select("project_id")
        .distinct()
    )
    orphans = evaluated.join(known_projects, on="project_id", how="left_anti").withColumn(
        "dq_reasons", F.array_union("dq_reasons", F.array(F.lit("orphan_project_id")))
    )
    matched = evaluated.join(known_projects, on="project_id", how="left_semi")
    checked = matched.unionByName(orphans)

    quarantine_df = checked.filter(F.size("dq_reasons") > 0)
    fx_daily = spark.read.format("delta").load(
        join_path(lakehouse_root, "silver", "silver_fx_rates_daily")
    )
    valid_df = (
        _keep_first_per_key(checked.filter(F.size("dq_reasons") == 0), "inv_id")
        .transform(lambda df: add_amount_eur(df, fx_daily, "issued_date", "inv"))
        .withColumn("is_credit_note", F.col("amount") < 0)
        .withColumn("is_paid", F.col("paid_date").isNotNull())
        .withColumn("net_amount_eur", F.col("amount_eur"))
        .withColumn("_silver_updated_at", F.current_timestamp())
        .select(
            "inv_id",
            "client_id",
            "project_id",
            "amount",
            "currency",
            "issued_date",
            "paid_date",
            "is_paid",
            "is_credit_note",
            "fx_rate_to_eur",
            "amount_eur",
            "net_amount_eur",
            "_batch_date",
            "_source_file",
            "_silver_updated_at",
        )
    ).localCheckpoint(eager=True)

    replace_batch_partition(valid_df, silver_path, batch_date)
    replace_batch_partition(quarantine_df, quarantine_path, batch_date)

    valid_n, quarantined_n = valid_df.count(), quarantine_df.count()
    orphan_n = orphans.count()
    common = {"run_id": run_id, "batch_date": batch_date, "layer": "silver"}
    results = [
        check_not_null(valid_df, "inv_id", table_name="silver_invoices", **common),
        check_unique_key(valid_df, ["inv_id"], table_name="silver_invoices", **common),
        check_zero_null_eur(valid_df, "amount_eur", table_name="silver_invoices", **common),
        check_quarantine_ratio(
            valid_n,
            quarantined_n,
            max_ratio=MAX_QUARANTINE_RATIO,
            table_name="silver_invoices",
            **common,
        ),
        check_quarantine_ratio(
            valid_n + quarantined_n - orphan_n,
            orphan_n,
            max_ratio=MAX_ORPHAN_RATIO,
            table_name="silver_invoices",
            check_name="orphan_invoice_rate",
            **common,
        ),
    ]
    return valid_df, quarantine_df, results


# ----------------------------------------------------------------------------- expenses


def transform_expenses(
    spark: SparkSession, batch_date: str, lakehouse_root: str, run_id: str
) -> tuple[DataFrame, DataFrame, list[DQCheckResult]]:
    bronze_path = join_path(lakehouse_root, "bronze", "expenses")
    silver_path = join_path(lakehouse_root, "silver", "silver_expenses")
    quarantine_path = join_path(lakehouse_root, "silver_quarantine", "expenses")

    normalized = (
        _load_batch(spark, bronze_path, batch_date)
        .withColumn("exp_id", _clean_str("exp_id"))
        .withColumn("department", _clean_str("department"))
        .withColumn("category", _clean_str("category"))
        .withColumn("currency", F.upper(_clean_str("currency")))
        .withColumn("expense_date", parse_mixed_date("expense_date"))
    )
    evaluated = _with_reasons(
        normalized,
        F.when(F.col("exp_id").isNull(), F.lit("missing_exp_id")),
        F.when(F.col("expense_date").isNull(), F.lit("invalid_expense_date")),
        F.when(
            F.col("department").isNull() | ~F.col("department").isin(*VALID_DEPARTMENTS),
            F.lit("invalid_department"),
        ),
        F.when(
            F.col("category").isNull() | ~F.col("category").isin(*VALID_EXPENSE_CATEGORIES),
            F.lit("invalid_category"),
        ),
        F.when(
            F.col("currency").isNull() | ~F.col("currency").isin(*VALID_CURRENCIES),
            F.lit("invalid_currency"),
        ),
        F.when(F.col("amount").isNull() | (F.col("amount") <= 0), F.lit("invalid_amount")),
    )

    quarantine_df = evaluated.filter(F.size("dq_reasons") > 0)
    candidates = evaluated.filter(F.size("dq_reasons") == 0)
    fx_daily = spark.read.format("delta").load(
        join_path(lakehouse_root, "silver", "silver_fx_rates_daily")
    )
    valid_df = (
        _keep_first_per_key(candidates, "exp_id")
        .transform(lambda df: add_amount_eur(df, fx_daily, "expense_date", "exp"))
        .withColumn("_silver_updated_at", F.current_timestamp())
        .select(
            "exp_id",
            "department",
            "category",
            "amount",
            "currency",
            "expense_date",
            "fx_rate_to_eur",
            "amount_eur",
            "_batch_date",
            "_source_file",
            "_silver_updated_at",
        )
    ).localCheckpoint(eager=True)

    replace_batch_partition(valid_df, silver_path, batch_date)
    replace_batch_partition(quarantine_df, quarantine_path, batch_date)

    candidates_n, valid_n, quarantined_n = (
        candidates.count(),
        valid_df.count(),
        quarantine_df.count(),
    )
    duplicates_removed = candidates_n - valid_n
    common = {"run_id": run_id, "batch_date": batch_date, "layer": "silver"}
    results = [
        check_not_null(valid_df, "exp_id", table_name="silver_expenses", **common),
        check_unique_key(valid_df, ["exp_id"], table_name="silver_expenses", **common),
        check_zero_null_eur(valid_df, "amount_eur", table_name="silver_expenses", **common),
        check_quarantine_ratio(
            candidates_n,
            quarantined_n,
            max_ratio=MAX_QUARANTINE_RATIO,
            table_name="silver_expenses",
            **common,
        ),
        check_quarantine_ratio(
            valid_n,
            duplicates_removed,
            max_ratio=MAX_DUPLICATE_RATIO,
            table_name="silver_expenses",
            check_name="duplicate_rate",
            **common,
        ),
    ]
    return valid_df, quarantine_df, results


# ----------------------------------------------------------------------------- entrypoint


def run(
    spark: SparkSession,
    batch_date: str,
    lakehouse_root: str | None = None,
    run_id: str | None = None,
) -> dict[str, tuple[DataFrame, DataFrame]]:
    """Build `silver_invoices` and `silver_expenses` for one `batch_date`, then enforce DQ."""
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)
    resolved_run_id = run_id or f"manual-{batch_date}-{uuid.uuid4().hex[:8]}"

    inv_valid, inv_q, inv_results = transform_invoices(
        spark, batch_date, resolved_lakehouse, resolved_run_id
    )
    exp_valid, exp_q, exp_results = transform_expenses(
        spark, batch_date, resolved_lakehouse, resolved_run_id
    )

    results = inv_results + exp_results
    write_audit_results(spark, results, join_path(resolved_lakehouse, "dq", "dq_audit_results"))
    enforce_dq_gate(results)
    return {"silver_invoices": (inv_valid, inv_q), "silver_expenses": (exp_valid, exp_q)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver_invoices and silver_expenses.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    parser.add_argument("--run-id", default=None, help="Orchestrator run identifier.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("silver-transform-invoices-expenses")
    try:
        out = run(spark, args.batch_date, args.lakehouse_root, args.run_id)
        for table, (valid, quarantined) in out.items():
            n_valid, n_quarantined = valid.count(), quarantined.count()
            print(f"{table} {args.batch_date}: valid={n_valid}, quarantined={n_quarantined}")
    finally:
        spark.stop()
