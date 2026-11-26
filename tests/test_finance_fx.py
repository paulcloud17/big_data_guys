"""Tests for Bronze finance ingestion and `silver_fx_rates_daily` (calendar spine + LOCF).

The synthetic FX fixture mirrors the real 14-day sanity slice: 2026-01-01..2026-01-14,
weekday-only files (no files on 01-03, 01-04, 01-10, 01-11), 6 currency pairs per day
-> 60 raw rows in Bronze and 84 rows (14 x 6) in Silver with 0 NULL rates.
"""

from __future__ import annotations

import json
import shutil
from datetime import date, timedelta
from pathlib import Path

import pytest

from dq.framework import DQGateFailedError
from spark_jobs.bronze.ingest_finance import (
    FX_RATES,
    INVOICES,
    ingest_daily_dataset,
)
from spark_jobs.common.spark_session import join_path
from spark_jobs.silver import transform_fx_rates

PAIRS = [
    ("EUR", "USD"),
    ("USD", "EUR"),
    ("EUR", "CZK"),
    ("CZK", "EUR"),
    ("USD", "CZK"),
    ("CZK", "USD"),
]
START = date(2026, 1, 1)  # Thursday
DAYS = [START + timedelta(days=i) for i in range(14)]
WEEKEND_DAYS = {date(2026, 1, 3), date(2026, 1, 4), date(2026, 1, 10), date(2026, 1, 11)}


def _rate(day: date, pair_idx: int) -> float:
    """Distinct, day-dependent rate so we can tell which day a value came from."""
    return round(1.0 + pair_idx + day.day / 100, 4)


def _write_fx_file(landing_root: Path, day: date, rows: list[dict]) -> None:
    part_dir = landing_root / "fx_rates" / f"date={day.isoformat()}"
    part_dir.mkdir(parents=True, exist_ok=True)
    (part_dir / "part-000.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")


def _fx_rows(day: date) -> list[dict]:
    return [
        {"rate_date": day.isoformat(), "currency_from": f, "currency_to": t, "rate": _rate(day, i)}
        for i, (f, t) in enumerate(PAIRS)
    ]


def _ingest_fx_days(spark, landing_root: Path, lakehouse_root: Path, days: list[date]) -> None:
    for day in days:
        ingest_daily_dataset(
            spark, FX_RATES, day.isoformat(), str(landing_root), str(lakehouse_root)
        )


@pytest.fixture(scope="module")
def fx_bronze_template(spark, tmp_path_factory) -> tuple[Path, Path]:
    """Landing files + Bronze fx_rates for all 14 days, built once and copied per test."""
    base = tmp_path_factory.mktemp("fx_template")
    landing_root, lakehouse_root = base / "landing", base / "lakehouse"
    for day in DAYS:
        if day not in WEEKEND_DAYS:
            _write_fx_file(landing_root, day, _fx_rows(day))
    _ingest_fx_days(spark, landing_root, lakehouse_root, DAYS)
    return landing_root, lakehouse_root


@pytest.fixture()
def fx_env(fx_bronze_template, tmp_path: Path) -> tuple[Path, Path]:
    """Fresh copy of the 14-day landing + Bronze state for one test."""
    landing_src, lakehouse_src = fx_bronze_template
    landing_root, lakehouse_root = tmp_path / "landing", tmp_path / "lakehouse"
    shutil.copytree(landing_src, landing_root)
    shutil.copytree(lakehouse_src, lakehouse_root)
    return landing_root, lakehouse_root


def _load(spark, lakehouse_root: Path, *parts: str):
    return spark.read.format("delta").load(join_path(str(lakehouse_root), *parts))


# ---------------------------------------------------------------- Bronze


def test_bronze_fx_skips_weekends_and_is_idempotent(spark, fx_env) -> None:
    landing_root, lakehouse_root = fx_env

    # Weekend: no file -> skipped gracefully, nothing written.
    assert (
        ingest_daily_dataset(spark, FX_RATES, "2026-01-03", str(landing_root), str(lakehouse_root))
        is None
    )
    # Re-ingest a weekday that is already in Bronze -> must not duplicate.
    _ingest_fx_days(spark, landing_root, lakehouse_root, [date(2026, 1, 2)])

    bronze = _load(spark, lakehouse_root, "bronze", "fx_rates")
    assert bronze.count() == 60  # 10 weekdays x 6 pairs
    assert bronze.select("_batch_date").distinct().count() == 10
    assert bronze.filter("_batch_date = DATE '2026-01-02'").count() == 6
    assert (
        bronze.filter("_batch_date IS NULL OR _ingested_at IS NULL OR _source_file IS NULL").count()
        == 0
    )


def test_bronze_invoices_keep_raw_dates_and_require_file(spark, tmp_path: Path) -> None:
    landing_root, lakehouse_root = tmp_path / "landing", tmp_path / "lakehouse"

    with pytest.raises(FileNotFoundError):
        ingest_daily_dataset(spark, INVOICES, "2026-01-05", str(landing_root), str(lakehouse_root))

    part_dir = landing_root / "invoices" / "date=2026-01-05"
    part_dir.mkdir(parents=True)
    lines = [
        {
            "inv_id": "INV_1",
            "client_id": "CLI_0002",
            "project_id": "PRJ_0001",
            "amount": 100.0,
            "currency": "EUR",
            "issued_at": "05.01.2026",
            "paid_at": None,
        },
        {
            "inv_id": "INV_2",
            "client_id": "CLI_0002",
            "project_id": "PRJ_0001",
            "amount": -20.0,
            "currency": "USD",
            "issued_at": "2026-01-05",
            "paid_at": "2026-01-05",
        },
    ]
    (part_dir / "part-000.jsonl").write_text(
        "\n".join(json.dumps(line) for line in lines) + "\n", encoding="utf-8"
    )
    for _ in range(2):  # run twice -> idempotent
        ingest_daily_dataset(spark, INVOICES, "2026-01-05", str(landing_root), str(lakehouse_root))

    bronze = _load(spark, lakehouse_root, "bronze", "invoices")
    rows = {r["inv_id"]: r for r in bronze.collect()}
    assert len(rows) == 2 and bronze.count() == 2
    assert rows["INV_1"]["issued_at"] == "05.01.2026"  # raw string untouched
    assert rows["INV_1"]["paid_at"] is None
    assert rows["INV_2"]["amount"] == -20.0  # credit note kept as-is


# ---------------------------------------------------------------- Silver FX


def test_silver_fx_spine_locf_matches_sanity_counts(spark, fx_env) -> None:
    _, lakehouse_root = fx_env
    for run_id in ("run-1", "run-2"):  # second run must give the identical table
        transform_fx_rates.run(spark, "2026-01-14", str(lakehouse_root), run_id=run_id)

    silver = _load(spark, lakehouse_root, "silver", "silver_fx_rates_daily")
    assert silver.count() == 84
    assert silver.filter("rate IS NULL").count() == 0
    assert silver.select("rate_date", "currency_from", "currency_to").distinct().count() == 84
    assert silver.filter("is_forward_filled").count() == 4 * 6

    # Saturday & Sunday inherit Friday 2026-01-02's rate (LOCF).
    friday = date(2026, 1, 2)
    weekend = silver.filter(
        "currency_from = 'EUR' AND currency_to = 'USD' "
        "AND rate_date IN (DATE '2026-01-03', DATE '2026-01-04')"
    ).collect()
    assert len(weekend) == 2
    for row in weekend:
        assert row["rate"] == _rate(friday, 0)
        assert row["is_forward_filled"] is True
        assert row["source_rate_date"] == friday

    # Weekday rows are the real observed rates.
    monday = silver.filter(
        "currency_from = 'EUR' AND currency_to = 'USD' AND rate_date = DATE '2026-01-05'"
    ).first()
    assert monday["rate"] == _rate(date(2026, 1, 5), 0)
    assert monday["is_forward_filled"] is False

    audit = _load(spark, lakehouse_root, "dq", "dq_audit_results").filter(
        "table_name = 'silver_fx_rates_daily'"
    )
    assert {(r["check_name"], r["status"], r["run_id"]) for r in audit.collect()} == {
        ("not_null_rate", "PASS", "run-2"),
        ("complete_unique_spine", "PASS", "run-2"),
    }


def test_silver_fx_weekend_batch_date_and_backfill_never_truncate(spark, fx_env) -> None:
    _, lakehouse_root = fx_env

    # Batch on a Sunday: spine ends on that Sunday, filled from Friday 2026-01-09.
    transform_fx_rates.run(spark, "2026-01-11", str(lakehouse_root), run_id="sun")
    silver = _load(spark, lakehouse_root, "silver", "silver_fx_rates_daily")
    assert silver.count() == 11 * 6
    sunday = silver.filter(
        "rate_date = DATE '2026-01-11' AND currency_from = 'CZK' AND currency_to = 'USD'"
    ).first()
    assert sunday["rate"] == _rate(date(2026, 1, 9), 5)
    assert sunday["source_rate_date"] == date(2026, 1, 9)

    # Process up to 01-14, then backfill an older day: the table must keep all 14 days.
    transform_fx_rates.run(spark, "2026-01-14", str(lakehouse_root), run_id="latest")
    transform_fx_rates.run(spark, "2026-01-05", str(lakehouse_root), run_id="backfill")
    silver = _load(spark, lakehouse_root, "silver", "silver_fx_rates_daily")
    assert silver.count() == 84
    assert silver.filter("rate IS NULL").count() == 0
    # ...and later days must keep their real observed rates (not stale forward-fills).
    last_day = silver.filter(
        "rate_date = DATE '2026-01-14' AND currency_from = 'EUR' AND currency_to = 'USD'"
    ).first()
    assert last_day["rate"] == _rate(date(2026, 1, 14), 0)
    assert last_day["is_forward_filled"] is False
    assert silver.filter("is_forward_filled").count() == 4 * 6  # only the 4 weekend days


def test_bronze_fx_missing_weekday_file_fails(spark, fx_env) -> None:
    landing_root, lakehouse_root = fx_env
    # 2026-01-15 is a Thursday and has no FX file -> must not be silently skipped.
    with pytest.raises(FileNotFoundError, match="fx_rates"):
        ingest_daily_dataset(spark, FX_RATES, "2026-01-15", str(landing_root), str(lakehouse_root))


def test_silver_fx_missing_pair_fails_hard_and_keeps_previous_table(spark, fx_env) -> None:
    landing_root, lakehouse_root = fx_env
    transform_fx_rates.run(spark, "2026-01-14", str(lakehouse_root), run_id="good")

    # Re-deliver every weekday without the CZK->USD pair.
    for day in DAYS:
        if day not in WEEKEND_DAYS:
            rows = [r for r in _fx_rows(day) if (r["currency_from"], r["currency_to"]) != PAIRS[5]]
            _write_fx_file(landing_root, day, rows)
    _ingest_fx_days(spark, landing_root, lakehouse_root, DAYS)

    with pytest.raises(DQGateFailedError, match="not_null_rate"):
        transform_fx_rates.run(spark, "2026-01-14", str(lakehouse_root), run_id="bad")

    # Checks run before writing -> the previous good table is untouched.
    silver = _load(spark, lakehouse_root, "silver", "silver_fx_rates_daily")
    assert silver.count() == 84
    assert silver.filter("rate IS NULL").count() == 0
    audit = _load(spark, lakehouse_root, "dq", "dq_audit_results").filter(
        "table_name = 'silver_fx_rates_daily' AND check_name = 'not_null_rate'"
    )
    failed = audit.first()
    assert (failed["run_id"], failed["status"], failed["failed_count"]) == ("bad", "FAIL", 14)


def test_silver_fx_quarantines_bad_rows_and_fills_the_gap(spark, fx_env) -> None:
    landing_root, lakehouse_root = fx_env

    # Corrupt 2026-01-07 (Wednesday): negative EUR->USD rate + an unknown currency row.
    bad_day = date(2026, 1, 7)
    rows = _fx_rows(bad_day)
    rows[0]["rate"] = -1.0
    rows.append(
        {
            "rate_date": bad_day.isoformat(),
            "currency_from": "GBP",
            "currency_to": "EUR",
            "rate": 1.15,
        }
    )
    _write_fx_file(landing_root, bad_day, rows)
    _ingest_fx_days(spark, landing_root, lakehouse_root, [bad_day])

    transform_fx_rates.run(spark, "2026-01-14", str(lakehouse_root), run_id="q")

    quarantine = _load(spark, lakehouse_root, "silver_quarantine", "fx_rates")
    reasons = sorted(tuple(r["dq_reasons"]) for r in quarantine.collect())
    assert reasons == [("invalid_currency_from",), ("invalid_rate",)]

    silver = _load(spark, lakehouse_root, "silver", "silver_fx_rates_daily")
    assert silver.count() == 84  # GBP pair never enters the spine
    gap = silver.filter(
        "rate_date = DATE '2026-01-07' AND currency_from = 'EUR' AND currency_to = 'USD'"
    ).first()
    assert gap["rate"] == _rate(date(2026, 1, 6), 0)  # filled from Tuesday
    assert gap["is_forward_filled"] is True

    # Fix the file and re-run: the stale quarantine rows disappear (idempotent).
    _write_fx_file(landing_root, bad_day, _fx_rows(bad_day))
    _ingest_fx_days(spark, landing_root, lakehouse_root, [bad_day])
    transform_fx_rates.run(spark, "2026-01-14", str(lakehouse_root), run_id="q-fixed")
    assert _load(spark, lakehouse_root, "silver_quarantine", "fx_rates").count() == 0
