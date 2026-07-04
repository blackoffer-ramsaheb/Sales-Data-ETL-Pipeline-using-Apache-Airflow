"""
transform.py
------------
Transforms validated sales records into a clean, analytics-ready format
and persists the result to data/processed/sales_clean.csv.

Transformations applied:
  1. Cast ``quantity`` to int and ``price`` to float.
  2. Compute ``total`` = quantity × price (rounded to 2 decimal places).
  3. Normalise ``product`` and ``customer`` to title-case.
  4. Parse ``date`` into ISO-8601 format (YYYY-MM-DD), rejecting unparsable rows.
  5. Drop the internal ``_validation_errors`` key if present.
  6. Write the final DataFrame to data/processed/sales_clean.csv.
"""

import csv
import logging
import os
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

# Output column order
OUTPUT_COLUMNS = [
    "order_id",
    "product",
    "quantity",
    "price",
    "total",
    "date",
    "customer",
]

# Common date formats the CSV might contain
DATE_FORMATS = ["%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y"]


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _get_processed_path() -> str:
    """Resolve path for the cleaned CSV output file."""
    container_path = "/opt/airflow/data/processed/sales_clean.csv"
    if os.path.exists("/opt/airflow"):
        return container_path

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    return os.path.join(project_root, "data", "processed", "sales_clean.csv")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_date(raw_date: str) -> str | None:
    """
    Attempt to parse *raw_date* using several common formats.

    Returns the date as an ISO-8601 string on success, or None on failure.
    """
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw_date.strip(), fmt).strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            continue
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def transform_data(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Apply all transformation rules to the validated records.

    Parameters
    ----------
    records : list[dict[str, Any]]
        Validated rows from validate.validate_data().

    Returns
    -------
    list[dict[str, Any]]
        Transformed rows; each row contains exactly the OUTPUT_COLUMNS plus
        a ``total`` field.

    Raises
    ------
    ValueError
        If the record list is empty.
    RuntimeError
        If every record fails transformation (e.g., all dates unparseable).
    """
    if not records:
        raise ValueError("Transformation received an empty record list.")

    logger.info("Starting transformation on %d validated records.", len(records))

    transformed: list[dict[str, Any]] = []
    skipped = 0

    for idx, row in enumerate(records, start=1):
        order_id = row.get("order_id", f"row-{idx}")

        # ── Parse date ───────────────────────────────────────────────────────
        parsed_date = _parse_date(str(row.get("date", "")))
        if parsed_date is None:
            logger.warning(
                "Row %d (order_id=%s) skipped — unparseable date '%s'.",
                idx,
                order_id,
                row.get("date"),
            )
            skipped += 1
            continue

        # ── Type conversions ─────────────────────────────────────────────────
        try:
            quantity = int(row["quantity"])
            price = round(float(row["price"]), 2)
        except (TypeError, ValueError, KeyError) as exc:
            logger.warning(
                "Row %d (order_id=%s) skipped — type conversion failed: %s",
                idx,
                order_id,
                exc,
            )
            skipped += 1
            continue

        # ── Derived field ────────────────────────────────────────────────────
        total = round(quantity * price, 2)

        # ── Normalise strings ────────────────────────────────────────────────
        product  = str(row.get("product",  "")).strip().title()
        customer = str(row.get("customer", "")).strip().title()

        transformed.append(
            {
                "order_id": str(order_id).strip(),
                "product":  product,
                "quantity": quantity,
                "price":    price,
                "total":    total,
                "date":     parsed_date,
                "customer": customer,
            }
        )

    logger.info(
        "Transformation complete — Processed: %d | Skipped: %d | Total input: %d",
        len(transformed),
        skipped,
        len(records),
    )

    if not transformed:
        raise RuntimeError(
            "No records survived the transformation step. Pipeline cannot continue."
        )

    # ── Persist to processed CSV ─────────────────────────────────────────────
    output_path = _get_processed_path()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(transformed)

    logger.info(
        "Clean data written to '%s' (%d rows).",
        output_path,
        len(transformed),
    )

    return transformed
