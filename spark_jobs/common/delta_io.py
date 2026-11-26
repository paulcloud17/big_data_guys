"""Shared Delta Lake write helpers."""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame


def replace_batch_partition(
    df: DataFrame, target_path: str, batch_date: str, partition_col: str = "_batch_date"
) -> None:
    """Atomically replace one daily partition of a Delta table (idempotent re-runs).

    First run creates the table partitioned by `partition_col`; later runs use `replaceWhere`,
    which deletes the old rows of that day and inserts the new ones in a single Delta commit
    (a crash can never leave the day half-deleted, and re-runs never duplicate rows).
    """
    spark = df.sparkSession
    writer = df.write.format("delta").partitionBy(partition_col)
    if DeltaTable.isDeltaTable(spark, target_path):
        (
            writer.mode("overwrite")
            .option("replaceWhere", f"{partition_col} = DATE '{batch_date}'")
            .option("mergeSchema", "true")  # allow additive schema evolution
            .save(target_path)
        )
    else:
        writer.mode("overwrite").save(target_path)
