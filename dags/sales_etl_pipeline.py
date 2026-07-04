"""
sales_etl_pipeline.py
---------------------
Apache Airflow DAG — Sales ETL Pipeline (TaskFlow API)

Pipeline overview:
    extract_task → validate_task → transform_task → load_task → report_task

Key Airflow concepts demonstrated:
  • @dag  / @task decorators (TaskFlow API — no PythonOperator boilerplate)
  • Implicit XCom: return values from @task functions are automatically
    pushed to XCom; the next task receives them as function arguments.
  • retries + retry_delay: each task will retry up to 3 times with a
    5-minute delay between attempts.
  • catchup=False: the DAG will not back-fill missed runs when first enabled.
  • Business logic is cleanly separated into the scripts/ package; this
    file orchestrates only — it contains no ETL implementation.
"""

from __future__ import annotations

import logging
import sys
import os
from datetime import datetime, timedelta
from typing import Any

# ── Make the scripts/ package importable inside the container ───────────────
# Airflow mounts the project root at /opt/airflow/ so we add scripts/ to the
# Python path once so every task can import from it without repeating this.
SCRIPTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from airflow.decorators import dag, task

# Import business-logic modules from scripts/
from extract   import extract_sales_data
from validate  import validate_data
from transform import transform_data
from load      import load_to_postgres
from report    import generate_report

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Default task arguments (applied to every @task in this DAG)
# ---------------------------------------------------------------------------

DEFAULT_ARGS: dict[str, Any] = {
    "owner":          "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    dag_id="sales_etl_pipeline",
    description=(
        "End-to-end sales ETL: extract CSV → validate → transform → "
        "load PostgreSQL → generate summary report."
    ),
    default_args=DEFAULT_ARGS,
    schedule="@daily",          # Run once per day; change to a cron string as needed
    start_date=datetime(2024, 1, 1),
    catchup=False,              # Do NOT back-fill missed runs
    max_active_runs=1,          # Only one concurrent run at a time
    tags=["etl", "sales", "postgresql", "learning"],
)
def sales_etl_pipeline() -> None:
    """
    ## Sales ETL Pipeline

    Orchestrates a daily sales data pipeline:

    1. **Extract**  — Read `data/raw/sales.csv`
    2. **Validate** — Check for missing values, duplicates, invalid data
    3. **Transform** — Clean, type-cast, and compute `Total = Qty × Price`
    4. **Load**     — Upsert into PostgreSQL `sales_orders` table (sales_db)
    5. **Report**   — Query DB and write `data/reports/summary.txt`
    """

    # ── TASK 1: Extract ──────────────────────────────────────────────────────
    @task(task_id="extract_sales_csv")
    def extract_task() -> list[dict[str, Any]]:
        """
        Read the raw sales CSV from disk.

        Returns the entire CSV as a list of row-dictionaries via XCom.
        All values are raw strings; no conversions are applied yet.
        """
        logger.info("=" * 60)
        logger.info("TASK: extract_sales_csv — starting")
        logger.info("=" * 60)

        records = extract_sales_data()

        logger.info(
            "extract_task complete — %d raw records pushed to XCom.", len(records)
        )
        return records   # Airflow auto-pushes this via XCom

    # ── TASK 2: Validate ─────────────────────────────────────────────────────
    @task(task_id="validate_data")
    def validate_task(raw_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Validate the extracted records.

        Checks:
          • Missing values in required columns
          • Duplicate order IDs
          • Invalid quantity (must be > 0 integer)
          • Invalid price   (must be > 0 float)

        Receives *raw_records* automatically from XCom (pushed by extract_task).
        Returns only the rows that passed all checks.
        """
        logger.info("=" * 60)
        logger.info("TASK: validate_data — received %d records from extract.", len(raw_records))
        logger.info("=" * 60)

        valid_records = validate_data(raw_records)

        logger.info(
            "validate_task complete — %d valid records pushed to XCom "
            "(%d rejected).",
            len(valid_records),
            len(raw_records) - len(valid_records),
        )
        return valid_records

    # ── TASK 3: Transform ────────────────────────────────────────────────────
    @task(task_id="transform_data")
    def transform_task(valid_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Transform and enrich the validated records.

        Steps:
          • Cast quantity → int, price → float
          • Compute total = quantity × price
          • Normalise product & customer to title-case
          • Parse and normalise date to ISO-8601
          • Write clean records to data/processed/sales_clean.csv

        Receives *valid_records* from XCom (pushed by validate_task).
        """
        logger.info("=" * 60)
        logger.info("TASK: transform_data — received %d valid records.", len(valid_records))
        logger.info("=" * 60)

        transformed = transform_data(valid_records)

        logger.info(
            "transform_task complete — %d transformed records pushed to XCom.",
            len(transformed),
        )
        return transformed

    # ── TASK 4: Load ───────────────────────────────────────────────────────────────
    @task(task_id="load_to_postgres")
    def load_task(transformed_records: list[dict[str, Any]]) -> int:
        """
        Load transformed records into the PostgreSQL sales_db database.

        Strategy: INSERT … ON CONFLICT DO UPDATE — idempotent; safe to re-run.
        The table ``sales_orders`` is created if it doesn't exist yet.

        Returns the number of rows upserted (pushed to XCom for the
        report task to reference if needed).
        """
        logger.info("=" * 60)
        logger.info(
            "TASK: load_to_postgres — loading %d records.", len(transformed_records)
        )
        logger.info("=" * 60)

        rows_loaded = load_to_postgres(transformed_records)

        logger.info(
            "load_task complete — %d rows upserted into PostgreSQL sales_db.", rows_loaded
        )
        return rows_loaded

    # ── TASK 5: Report ───────────────────────────────────────────────────────
    @task(task_id="generate_summary_report")
    def report_task(rows_loaded: int) -> dict[str, Any]:
        """
        Generate a summary report from the SQLite database.

        Receives *rows_loaded* from XCom (ensures this task only runs after
        the load step succeeds).

        Queries:
          • Total orders & revenue
          • Average order value
          • Top selling product (by units)
          • Top 5 products by revenue
          • Monthly revenue breakdown

        Writes the report to data/reports/summary.txt.
        """
        logger.info("=" * 60)
        logger.info(
            "TASK: generate_summary_report — %d rows available in DB.", rows_loaded
        )
        logger.info("=" * 60)

        metrics = generate_report()

        logger.info("─" * 60)
        logger.info("REPORT METRICS SUMMARY:")
        logger.info("  Total Orders          : %s",  metrics["total_orders"])
        logger.info("  Total Revenue         : ₹%s", f"{metrics['total_revenue']:,.2f}")
        logger.info("  Average Order Value   : ₹%s", f"{metrics['avg_order_value']:,.2f}")
        logger.info(
            "  Top Selling Product   : %s (%s units)",
            metrics["top_product"],
            metrics["top_product_units"],
        )
        logger.info("  Report written to     : %s", metrics["report_path"])
        logger.info("─" * 60)

        return metrics

    # ── Wire up task dependencies (implicit via XCom return values) ──────────
    # Each task receives the previous task's return value as its argument.
    # Airflow resolves this automatically — no explicit set_downstream() needed.
    raw_records         = extract_task()
    valid_records       = validate_task(raw_records)
    transformed_records = transform_task(valid_records)
    rows_loaded         = load_task(transformed_records)
    _report_metrics     = report_task(rows_loaded)


# ── Instantiate the DAG ──────────────────────────────────────────────────────
# Calling the decorated function registers the DAG with the Airflow scheduler.
sales_etl_pipeline()
