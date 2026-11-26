"""Data Quality check library (Section 4.3 of `docs/STUDENT_ASSIGNMENT.md`).

Each function returns a `DQCheckResult` from `dq.framework`; persist them with
`write_audit_results` and enforce hard gates with `enforce_dq_gate`.

Implemented (needed by Finance Silver): `check_unique_key`, `check_zero_null_eur`,
`check_quarantine_ratio`.
TODO (DQ/orchestration owner): `check_non_empty_partition`, `check_scd2_no_overlaps`.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dq.framework import DQCheckResult, _parse_batch_date


def _status(failed: bool, severity: str) -> str:
    if not failed:
        return "PASS"
    return "WARN" if severity.upper() == "WARN" else "FAIL"


def _result(
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    check_name: str,
    severity: str,
    failed: bool,
    failed_count: int,
    total_count: int,
) -> DQCheckResult:
    return DQCheckResult(
        run_id=run_id,
        batch_date=_parse_batch_date(batch_date),
        layer=layer,
        table_name=table_name,
        check_name=check_name,
        severity=severity.upper(),
        status=_status(failed, severity),
        failed_count=int(failed_count),
        total_count=int(total_count),
        checked_at=datetime.now(timezone.utc),
    )


def check_unique_key(
    df: DataFrame,
    key_columns: Sequence[str],
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    severity: str = "ERROR",
) -> DQCheckResult:
    """Verify uniqueness of a primary/composite key; failed_count = surplus duplicate rows."""
    keys = list(key_columns)
    total = df.count()
    surplus = total - df.select(*keys).distinct().count()
    return _result(
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        check_name=f"unique_{'_'.join(keys)}",
        severity=severity,
        failed=surplus > 0,
        failed_count=surplus,
        total_count=total,
    )


def check_non_empty_partition(
    df: DataFrame,
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    allow_weekends_empty: bool = False,
    severity: str = "ERROR",
) -> DQCheckResult:
    """TODO: Verify mandatory partitions contain >0 rows (accounting for weekend fx_rates)."""
    raise NotImplementedError("TODO: Implement check_non_empty_partition in dq/checks.py")


def check_scd2_no_overlaps(
    df: DataFrame,
    *,
    run_id: str,
    batch_date: str | date,
    layer: str = "silver",
    table_name: str = "silver_employees_scd2",
    severity: str = "ERROR",
) -> DQCheckResult:
    """TODO: Verify no overlapping [valid_from, valid_to] intervals per emp_id."""
    raise NotImplementedError("TODO: Implement check_scd2_no_overlaps in dq/checks.py")


def check_zero_null_eur(
    df: DataFrame,
    eur_column: str,
    *,
    run_id: str,
    batch_date: str | date,
    layer: str,
    table_name: str,
    severity: str = "ERROR",
) -> DQCheckResult:
    """Verify zero NULL EUR amounts after FX conversion (invoices & expenses)."""
    counts = df.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(F.col(eur_column).isNull(), 1).otherwise(0)).alias("nulls"),
    ).first()
    nulls = counts["nulls"] or 0
    return _result(
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        check_name=f"zero_null_{eur_column}",
        severity=severity,
        failed=nulls > 0,
        failed_count=nulls,
        total_count=counts["total"],
    )


def check_quarantine_ratio(
    valid_count: int,
    quarantined_count: int,
    *,
    max_ratio: float = 0.08,
    run_id: str,
    batch_date: str | date,
    layer: str = "silver",
    table_name: str,
    severity: str = "WARN",
    check_name: str = "quarantine_ratio",
) -> DQCheckResult:
    """Flag when quarantined_count / (valid_count + quarantined_count) > max_ratio.

    Also reusable for other rate metrics (e.g. orphan rate, duplicate rate) via `check_name`.
    """
    total = valid_count + quarantined_count
    ratio = quarantined_count / total if total else 0.0
    return _result(
        run_id=run_id,
        batch_date=batch_date,
        layer=layer,
        table_name=table_name,
        check_name=check_name,
        severity=severity,
        failed=ratio > max_ratio,
        failed_count=quarantined_count,
        total_count=total,
    )
