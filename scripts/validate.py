"""
validate.py
-----------
Validates the raw records extracted from the CSV file.

Checks performed:
  1. Missing / blank values in critical columns (order_id, product,
     quantity, price, date, customer).
  2. Duplicate order IDs.
  3. Invalid quantity  — must be a positive integer (> 0).
  4. Invalid price     — must be a positive float (> 0).

The function returns only the records that pass ALL checks and logs a
detailed report of every rejected row so that operators can investigate.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Columns that must never be blank
REQUIRED_COLUMNS = ["order_id", "product", "quantity", "price", "date", "customer"]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _is_blank(value: Any) -> bool:
    """Return True if the value is None, empty string, or whitespace-only."""
    return value is None or str(value).strip() == ""


def _is_positive_integer(value: Any) -> bool:
    """Return True if value can be cast to an integer greater than zero."""
    try:
        return int(value) > 0
    except (TypeError, ValueError):
        return False


def _is_positive_float(value: Any) -> bool:
    """Return True if value can be cast to a float greater than zero."""
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_data(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Validate raw records and return only those that pass every check.

    Parameters
    ----------
    records : list[dict[str, Any]]
        Raw rows as returned by extract.extract_sales_data().

    Returns
    -------
    list[dict[str, Any]]
        Records that passed all validation rules.

    Raises
    ------
    ValueError
        If the input list is empty (nothing to validate).
    RuntimeError
        If every single record is rejected (pipeline cannot continue).
    """
    if not records:
        raise ValueError("Validation received an empty record list — nothing to validate.")

    logger.info("Starting validation on %d raw records.", len(records))

    valid_records: list[dict[str, Any]] = []
    invalid_records: list[dict[str, Any]] = []
    seen_order_ids: set[str] = set()

    for idx, row in enumerate(records, start=1):
        order_id = str(row.get("order_id", "")).strip()
        reasons: list[str] = []

        # ── 1. Missing values ───────────────────────────────────────────────
        for col in REQUIRED_COLUMNS:
            if _is_blank(row.get(col)):
                reasons.append(f"missing value in '{col}'")

        # ── 2. Duplicate order ID ───────────────────────────────────────────
        if order_id and order_id in seen_order_ids:
            reasons.append(f"duplicate order_id '{order_id}'")

        # ── 3. Invalid quantity ─────────────────────────────────────────────
        if not _is_blank(row.get("quantity")) and not _is_positive_integer(row.get("quantity")):
            reasons.append(
                f"invalid quantity '{row.get('quantity')}' (must be a positive integer)"
            )

        # ── 4. Invalid price ────────────────────────────────────────────────
        if not _is_blank(row.get("price")) and not _is_positive_float(row.get("price")):
            reasons.append(
                f"invalid price '{row.get('price')}' (must be a positive number)"
            )

        # ── Decision ────────────────────────────────────────────────────────
        if reasons:
            row["_validation_errors"] = "; ".join(reasons)
            invalid_records.append(row)
            logger.warning(
                "Row %d REJECTED (order_id=%s): %s",
                idx,
                order_id or "<missing>",
                "; ".join(reasons),
            )
        else:
            seen_order_ids.add(order_id)
            valid_records.append(row)

    # ── Summary log ─────────────────────────────────────────────────────────
    logger.info(
        "Validation complete — Passed: %d | Rejected: %d | Total: %d",
        len(valid_records),
        len(invalid_records),
        len(records),
    )

    if invalid_records:
        logger.warning(
            "Rejected rows summary:\n%s",
            "\n".join(
                f"  • {r.get('order_id','?')} — {r.get('_validation_errors','')}"
                for r in invalid_records
            ),
        )

    if not valid_records:
        raise RuntimeError(
            "All records were rejected during validation. "
            "Pipeline cannot continue. Please fix the source data."
        )

    return valid_records
