"""Bronze ingestion for the Finance domain (`fx_rates`, `invoices`, `expenses`).

Pattern (same for all three datasets):
- Read exactly one daily Hive-style partition
  `<landing_root>/<dataset>/date=<batch_date>/part-000.<ext>` with an explicit schema.
- Keep raw values unchanged (e.g. invoice `issued_at` / `paid_at` stay raw strings,
  `expense_date` stays a raw string; parsing happens in Silver).
- Append technical metadata `_batch_date`, `_ingested_at`, `_source_file`, plus
  `_source_row_id` (increasing in file order; one small daily file = one Spark partition)
  so Silver can apply a deterministic "keep the first occurrence" dedup rule.
- Write to `bronze/<dataset>` partitioned by `_batch_date`, atomically replacing only that
  `_batch_date` (Delta `replaceWhere`) -> re-running a day never duplicates rows.

`fx_rates` is delivered on weekdays only. A missing fx_rates file is expected on
Saturdays/Sundays and is skipped without failing; Silver forward-fills those days.
Missing `invoices` / `expenses` files are a real error and raise `FileNotFoundError`.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from spark_jobs.common.delta_io import replace_batch_partition
from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
    join_path,
)

FX_RATES_RAW_SCHEMA = StructType(
    [
        StructField("rate_date", StringType(), nullable=True),
        StructField("currency_from", StringType(), nullable=True),
        StructField("currency_to", StringType(), nullable=True),
        StructField("rate", DoubleType(), nullable=True),
    ]
)

INVOICES_RAW_SCHEMA = StructType(
    [
        StructField("inv_id", StringType(), nullable=True),
        StructField("client_id", StringType(), nullable=True),
        StructField("project_id", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("issued_at", StringType(), nullable=True),  # raw: ISO or DD.MM.YYYY
        StructField("paid_at", StringType(), nullable=True),  # raw, NULL = unpaid
    ]
)

EXPENSES_RAW_SCHEMA = StructType(
    [
        StructField("exp_id", StringType(), nullable=True),
        StructField("department", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("expense_date", StringType(), nullable=True),  # raw, parsed in Silver
    ]
)


@dataclass(frozen=True)
class DailyDataset:
    """Describes one daily-partitioned landing dataset."""

    name: str
    file_name: str
    schema: StructType
    reader: str  # "json_array" | "jsonl" | "csv"
    optional: bool = False  # True -> missing file is expected (e.g. weekend fx_rates)


FX_RATES = DailyDataset("fx_rates", "part-000.json", FX_RATES_RAW_SCHEMA, "json_array", True)
INVOICES = DailyDataset("invoices", "part-000.jsonl", INVOICES_RAW_SCHEMA, "jsonl")
EXPENSES = DailyDataset("expenses", "part-000.csv", EXPENSES_RAW_SCHEMA, "csv")
FINANCE_DATASETS = (FX_RATES, INVOICES, EXPENSES)


def _path_exists(spark: SparkSession, path: str) -> bool:
    """Check existence via Hadoop FS so it works for local paths and s3a:// alike."""
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(path)  # noqa: SLF001
    fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())  # noqa: SLF001
    return fs.exists(jvm_path)


def _read_raw(spark: SparkSession, dataset: DailyDataset, source_path: str) -> DataFrame:
    reader = spark.read.schema(dataset.schema)
    if dataset.reader == "json_array":
        # One JSON document holding an array of objects -> one row per array element.
        return reader.option("multiLine", "true").json(source_path)
    if dataset.reader == "jsonl":
        return reader.json(source_path)
    if dataset.reader == "csv":
        return reader.option("header", "true").csv(source_path)
    raise ValueError(f"Unknown reader '{dataset.reader}' for dataset {dataset.name}")


def ingest_daily_dataset(
    spark: SparkSession,
    dataset: DailyDataset,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> DataFrame | None:
    """Ingest one dataset for one `batch_date`. Returns None if an optional file is absent."""
    resolved_landing = get_landing_root(landing_root)
    resolved_lakehouse = get_lakehouse_root(lakehouse_root)

    source_path = join_path(resolved_landing, dataset.name, f"date={batch_date}", dataset.file_name)
    target_path = join_path(resolved_lakehouse, "bronze", dataset.name)

    if not _path_exists(spark, source_path):
        if dataset.optional:
            print(
                f"[bronze] {dataset.name}: no file for {batch_date} (expected on weekends) - skip"
            )
            return None
        raise FileNotFoundError(f"Missing mandatory landing file: {source_path}")

    bronze_df = (
        _read_raw(spark, dataset, source_path)
        .withColumn("_batch_date", F.to_date(F.lit(batch_date)))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_source_row_id", F.monotonically_increasing_id())
    )
    replace_batch_partition(bronze_df, target_path, batch_date)
    return bronze_df


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> dict[str, int]:
    """Ingest `fx_rates`, `invoices` and `expenses` for `batch_date`; return row counts."""
    counts: dict[str, int] = {}
    for dataset in FINANCE_DATASETS:
        df = ingest_daily_dataset(spark, dataset, batch_date, landing_root, lakehouse_root)
        counts[dataset.name] = 0 if df is None else df.count()
    return counts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Finance datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-finance")
    try:
        row_counts = run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
        print(f"Bronze finance ingested for batch_date={args.batch_date}: {row_counts}")
    finally:
        spark.stop()
