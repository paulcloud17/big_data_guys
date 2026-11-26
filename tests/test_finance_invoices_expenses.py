"""Tests for `silver_projects`, `silver_invoices`, `silver_expenses` and the Finance DQ checks."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from dq.framework import DQGateFailedError
from spark_jobs.bronze.ingest_finance import EXPENSES, FX_RATES, INVOICES, ingest_daily_dataset
from spark_jobs.bronze.ingest_reference import ingest_projects
from spark_jobs.common.spark_session import join_path
from spark_jobs.silver import transform_fx_rates, transform_invoices_expenses, transform_projects

SAT = "2026-01-03"  # weekend batch: invoices/expenses exist, FX must come from Friday
MON = "2026-01-05"
FX = {  # currency_from -> rate to EUR, per weekday
    "2026-01-02": {"USD": 0.9, "CZK": 0.04},
    "2026-01-05": {"USD": 0.8, "CZK": 0.05},
}


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _fx_file(landing: Path, day: str) -> None:
    rates = FX[day]
    rows = []
    for cur, to_eur in rates.items():
        rows.append({"rate_date": day, "currency_from": cur, "currency_to": "EUR", "rate": to_eur})
        rows.append(
            {"rate_date": day, "currency_from": "EUR", "currency_to": cur, "rate": 1 / to_eur}
        )
    _write(landing / "fx_rates" / f"date={day}" / "part-000.json", json.dumps(rows))


def _invoices_file(landing: Path, day: str, rows: list[dict]) -> None:
    _write(
        landing / "invoices" / f"date={day}" / "part-000.jsonl",
        "\n".join(json.dumps(r) for r in rows) + "\n",
    )


def _expenses_file(landing: Path, day: str, lines: list[str]) -> None:
    header = "exp_id,department,category,amount,currency,expense_date"
    _write(
        landing / "expenses" / f"date={day}" / "part-000.csv", "\n".join([header, *lines]) + "\n"
    )


def _inv(inv_id, project, amount, currency, issued, paid=None) -> dict:
    return {
        "inv_id": inv_id,
        "client_id": "CLI_0002",
        "project_id": project,
        "amount": amount,
        "currency": currency,
        "issued_at": issued,
        "paid_at": paid,
    }


SAT_INVOICES = [
    _inv("INV_1", "PRJ_0001", 100.0, "EUR", "2026-01-03"),  # open invoice
    _inv("INV_2", "PRJ_0002", -50.0, "USD", "03.01.2026", "05.01.2026"),  # credit note, EU dates
    _inv("INV_3", "PRJ_ORPHAN_0001", 1000.0, "CZK", "2026-01-03"),  # orphan
    _inv("INV_4", "PRJ_0001", 10.0, "EUR", "2026/01/03"),  # unparseable date
    _inv("INV_5", "PRJ_0001", 10.0, "GBP", "2026-01-03"),  # unknown currency
]
SAT_EXPENSES = [
    "EXP_1,Engineering,Training,100.0,USD,2026-01-03",  # original (kept)
    "EXP_1,Engineering,Training,100.0,USD,2026-01-03",  # exact duplicate
    "EXP_1,Engineering,Training,120.0,USD,2026-01-03",  # near duplicate
    "EXP_2,Data,Hardware,2500.0,CZK,2026-01-03",
    "EXP_3,Marketing,Hardware,10.0,EUR,2026-01-03",  # invalid department
    "EXP_4,Sales,Travel & Lodging,-5.0,EUR,2026-01-03",  # invalid amount
]


@pytest.fixture()
def env(spark, tmp_path: Path) -> tuple[Path, Path]:
    landing, lakehouse = tmp_path / "landing", tmp_path / "lakehouse"
    _write(
        landing / "projects" / "projects.csv",
        "project_id,client_id,budget_eur,status\n"
        "PRJ_0001,CLI_0002,1000.0,Active\n"
        "PRJ_0002,CLI_0002,500.0,Completed\n"
        "PRJ_0003,CLI_0002,500.0,Cancelled\n",  # invalid status -> quarantined
    )
    for day in FX:
        _fx_file(landing, day)
    _invoices_file(landing, SAT, SAT_INVOICES)
    _expenses_file(landing, SAT, SAT_EXPENSES)

    ingest_projects(spark, MON, str(landing), str(lakehouse))
    transform_projects.run(spark, MON, str(lakehouse), run_id="prj")
    for day in ("2026-01-02", SAT, "2026-01-04", MON):
        ingest_daily_dataset(spark, FX_RATES, day, str(landing), str(lakehouse))
    transform_fx_rates.run(spark, MON, str(lakehouse), run_id="fx")
    for ds in (INVOICES, EXPENSES):
        ingest_daily_dataset(spark, ds, SAT, str(landing), str(lakehouse))
    return landing, lakehouse


def _load(spark, lakehouse: Path, *parts: str):
    return spark.read.format("delta").load(join_path(str(lakehouse), *parts))


def test_silver_projects_quarantines_invalid_status(spark, env) -> None:
    _, lakehouse = env
    assert {
        r["project_id"] for r in _load(spark, lakehouse, "silver", "silver_projects").collect()
    } == {
        "PRJ_0001",
        "PRJ_0002",
    }
    q = _load(spark, lakehouse, "silver_quarantine", "projects").collect()
    assert [(r["project_id"], tuple(r["dq_reasons"])) for r in q] == [
        ("PRJ_0003", ("invalid_project_status",))
    ]


def test_silver_invoices_dates_credit_notes_orphans_fx(spark, env) -> None:
    _, lakehouse = env
    for run_id in ("r1", "r2"):  # re-run must be idempotent
        transform_invoices_expenses.run(spark, SAT, str(lakehouse), run_id=run_id)

    silver = {
        r["inv_id"]: r for r in _load(spark, lakehouse, "silver", "silver_invoices").collect()
    }
    assert set(silver) == {"INV_1", "INV_2"}

    inv1 = silver["INV_1"]
    assert inv1["issued_date"] == date(2026, 1, 3)
    assert inv1["paid_date"] is None and inv1["is_paid"] is False  # open invoice kept
    assert inv1["fx_rate_to_eur"] == 1.0 and inv1["amount_eur"] == 100.0

    inv2 = silver["INV_2"]  # European dates, credit note, USD on a Saturday -> Friday rate
    assert inv2["issued_date"] == date(2026, 1, 3)
    assert inv2["paid_date"] == date(2026, 1, 5)
    assert inv2["is_credit_note"] is True
    assert inv2["fx_rate_to_eur"] == pytest.approx(0.9)
    assert inv2["amount_eur"] == pytest.approx(-45.0)
    assert inv2["net_amount_eur"] == pytest.approx(-45.0)

    quarantine = _load(spark, lakehouse, "silver_quarantine", "invoices").collect()
    assert len(quarantine) == 3
    assert {r["inv_id"]: tuple(r["dq_reasons"]) for r in quarantine} == {
        "INV_3": ("orphan_project_id",),
        "INV_4": ("invalid_issued_date",),
        "INV_5": ("invalid_currency",),
    }

    audit = {
        r["check_name"]: r
        for r in _load(spark, lakehouse, "dq", "dq_audit_results")
        .filter("table_name = 'silver_invoices'")
        .collect()
    }
    assert {name: r["status"] for name, r in audit.items()} == {
        "not_null_inv_id": "PASS",
        "unique_inv_id": "PASS",
        "zero_null_amount_eur": "PASS",
        "quarantine_ratio": "WARN",  # 3/5 > 8%
        "orphan_invoice_rate": "WARN",  # 1/5 > 2%
    }
    assert {r["run_id"] for r in audit.values()} == {"r2"}
    assert audit["orphan_invoice_rate"]["failed_count"] == 1


def test_silver_expenses_dedup_first_occurrence_and_fx(spark, env) -> None:
    _, lakehouse = env
    for run_id in ("r1", "r2"):
        transform_invoices_expenses.run(spark, SAT, str(lakehouse), run_id=run_id)

    silver = {
        r["exp_id"]: r for r in _load(spark, lakehouse, "silver", "silver_expenses").collect()
    }
    assert set(silver) == {"EXP_1", "EXP_2"}
    assert silver["EXP_1"]["amount"] == 100.0  # first occurrence wins over the 120.0 re-send
    assert silver["EXP_1"]["amount_eur"] == pytest.approx(90.0)  # Friday USD rate
    assert silver["EXP_2"]["amount_eur"] == pytest.approx(100.0)  # 2500 CZK * 0.04

    quarantine = _load(spark, lakehouse, "silver_quarantine", "expenses").collect()
    assert {r["exp_id"]: tuple(r["dq_reasons"]) for r in quarantine} == {
        "EXP_3": ("invalid_department",),
        "EXP_4": ("invalid_amount",),
    }

    audit = {
        r["check_name"]: r
        for r in _load(spark, lakehouse, "dq", "dq_audit_results")
        .filter("table_name = 'silver_expenses'")
        .collect()
    }
    assert audit["duplicate_rate"]["failed_count"] == 2
    assert audit["duplicate_rate"]["status"] == "WARN"
    assert audit["zero_null_amount_eur"]["status"] == "PASS"
    assert audit["unique_exp_id"]["status"] == "PASS"


def test_batches_are_isolated_and_missing_fx_fails_hard(spark, env) -> None:
    landing, lakehouse = env
    transform_invoices_expenses.run(spark, SAT, str(lakehouse), run_id="sat")

    # Monday batch: one valid invoice; its issue date lies beyond the FX spine -> NULL EUR.
    _invoices_file(landing, MON, [_inv("INV_9", "PRJ_0001", 10.0, "USD", "2026-01-09")])
    _expenses_file(landing, MON, ["EXP_9,Data,Training,10.0,EUR,2026-01-05"])
    for ds in (INVOICES, EXPENSES):
        ingest_daily_dataset(spark, ds, MON, str(landing), str(lakehouse))

    with pytest.raises(DQGateFailedError, match="zero_null_amount_eur"):
        transform_invoices_expenses.run(spark, MON, str(lakehouse), run_id="mon")

    # Saturday's partition is untouched by the Monday run.
    invoices = _load(spark, lakehouse, "silver", "silver_invoices")
    assert invoices.filter(f"_batch_date = DATE '{SAT}'").count() == 2
    expenses = _load(spark, lakehouse, "silver", "silver_expenses")
    assert expenses.filter(f"_batch_date = DATE '{SAT}'").count() == 2
    assert expenses.filter(f"_batch_date = DATE '{MON}'").count() == 1
