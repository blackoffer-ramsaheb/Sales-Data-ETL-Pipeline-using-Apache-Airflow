"""
extract.py
----------
Responsible for reading the raw sales CSV from the data/raw/ directory
and returning the records as a list of dictionaries.

This module is intentionally free of any Airflow imports so that it can
be unit-tested in isolation and reused outside of the DAG context.
"""

import csv
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _get_raw_data_path() -> str:
    """
    Resolve the absolute path to the raw CSV file.

    The function supports two runtime environments:
      1. Inside the Airflow Docker container  → /opt/airflow/data/raw/sales.csv
      2. On the host machine (local dev / tests) → <project_root>/data/raw/sales.csv
    """
    container_path = "/opt/airflow/data/raw/sales.csv"
    if os.path.exists(container_path):
        return container_path

    # Fall back to a path relative to this script file (works locally)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)           # one level up from scripts/
    return os.path.join(project_root, "data", "raw", "sales.csv")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract_sales_data() -> list[dict[str, Any]]:
    """
    Read the raw sales CSV and return a list of row dictionaries.

    Each dictionary maps column headers to string values exactly as they
    appear in the CSV file — no type conversions are applied here so that
    the Validate step can catch every kind of data quality issue.

    Returns
    -------
    list[dict[str, Any]]
        A list of row dictionaries; one dict per CSV row.

    Raises
    ------
    FileNotFoundError
        If the CSV file does not exist at the resolved path.
    ValueError
        If the CSV file is completely empty (no header row).
    """
    csv_path = _get_raw_data_path()

    logger.info("Resolved CSV path: %s", csv_path)

    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"Sales CSV not found at '{csv_path}'. "
            "Ensure data/raw/sales.csv is present before running the DAG."
        )

    records: list[dict[str, Any]] = []

    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)

        if reader.fieldnames is None:
            raise ValueError(f"CSV file is empty or has no header row: {csv_path}")

        logger.info("CSV columns detected: %s", list(reader.fieldnames))

        for row in reader:
            # Strip leading/trailing whitespace from every cell value
            cleaned_row = {k: (v.strip() if v else v) for k, v in row.items()}
            records.append(cleaned_row)

    logger.info("Extraction complete — %d raw rows loaded from CSV.", len(records))
    return records
