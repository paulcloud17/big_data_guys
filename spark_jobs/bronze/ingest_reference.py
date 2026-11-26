"""Bronze ingestion for full-load reference datasets: `projects` and `survey_questions`.

`projects` is implemented (needed by Finance for the invoice orphan check).

TODO (Student Deliverable - survey_questions owner):
- Ingest `<landing_root>/survey_questions/survey_questions.json` (multiline JSON array
  of 10 question definitions: `question_id`, `dimension`, `scale_min`, `scale_max`,
  `prompt_text`) into `<lakehouse_root>/bronze/survey_questions` (see `ingest_projects`).

Reference: `docs/STUDENT_ASSIGNMENT.md` (Sections 3.1, 3.3, 4.2.A) and `docs/SANITY_CHECKS.md`.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from spark_jobs.common.spark_session import (
    get_lakehouse_root,
    get_landing_root,
    get_spark_session,
    join_path,
)

PROJECTS_RAW_SCHEMA = StructType(
    [
        StructField("project_id", StringType(), nullable=True),
        StructField("client_id", StringType(), nullable=True),
        StructField("budget_eur", DoubleType(), nullable=True),
        StructField("status", StringType(), nullable=True),
    ]
)


def ingest_projects(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> DataFrame:
    """Read raw `projects.csv` and overwrite `bronze/projects` (full load -> idempotent)."""
    source_path = join_path(get_landing_root(landing_root), "projects", "projects.csv")
    target_path = join_path(get_lakehouse_root(lakehouse_root), "bronze", "projects")

    bronze_df = (
        spark.read.option("header", "true")
        .schema(PROJECTS_RAW_SCHEMA)
        .csv(source_path)
        .withColumn("_batch_date", F.to_date(F.lit(batch_date)))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
    bronze_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        target_path
    )
    return bronze_df


def ingest_survey_questions(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> DataFrame:
    """TODO (survey owner): ingest `survey_questions.json` into `bronze/survey_questions`."""
    raise NotImplementedError("TODO: Implement Bronze ingestion for survey_questions.")


def run(
    spark: SparkSession,
    batch_date: str,
    landing_root: str | None = None,
    lakehouse_root: str | None = None,
) -> None:
    """Ingest `projects` and `survey_questions` for `batch_date`."""
    ingest_projects(spark, batch_date, landing_root, lakehouse_root)
    ingest_survey_questions(spark, batch_date, landing_root, lakehouse_root)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest reference datasets into Bronze Delta.")
    parser.add_argument("--batch-date", required=True, help="Logical batch date (YYYY-MM-DD).")
    parser.add_argument("--landing-root", default=None, help="Override Landing Zone path.")
    parser.add_argument("--lakehouse-root", default=None, help="Override Lakehouse root path.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    spark = get_spark_session("bronze-ingest-reference")
    try:
        run(spark, args.batch_date, args.landing_root, args.lakehouse_root)
    finally:
        spark.stop()
