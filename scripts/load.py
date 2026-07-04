"""
load.py
-------
Loads the transformed sales records into a SQLite database.

Schema
------
Table: sales_orders
  order_id  TEXT PRIMARY KEY
  product   TEXT NOT NULL
  quantity  INTEGER NOT NULL
  price     REAL NOT NULL
  total     REAL NOT NULL
  date      TEXT NOT NULL          -- ISO-8601 (YYYY-MM-DD)
  customer  TEXT NOT NULL
  loaded_at TEXT NOT NULL          -- UTC timestamp of when the row was inserted

Strategy
--------
  • The table is created with ``CREATE TABLE IF NOT EXISTS`` so repeated
    DAG runs are safe.
  • Each batch uses ``INSERT OR REPLACE`` (UPSERT by primary key) so that
    re-runs do not produce duplicate rows — idempotent by design.
  • A single transaction is used for the entire batch to guarantee
    atomicity: either all rows land, or none do.
"""

import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sales_orders (
    order_id  TEXT PRIMARY KEY,
    product   TEXT    NOT NULL,
    quantity  INTEGER NOT NULL,
    price     REAL    NOT NULL,
    total     REAL    NOT NULL,
    date      TEXT    NOT NULL,
    customer  TEXT    NOT NULL,
    loaded_at TEXT    NOT NULL
);
"""

INSERT_SQL = """
INSERT OR REPLACE INTO sales_orders
    (order_id, product, quantity, price, total, date, customer, loaded_at)
VALUES
    (:order_id, :product, :quantity, :price, :total, :date, :customer, :loaded_at);
"""


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _get_db_path() -> str:
    """Resolve the absolute path to the SQLite database file."""
    container_path = "/opt/airflow/database/sales.db"
    if os.path.exists("/opt/airflow"):
        return container_path

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    return os.path.join(project_root, "database", "sales.db")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_to_sqlite(records: list[dict[str, Any]]) -> int:
    """
    Insert transformed records into the ``sales_orders`` SQLite table.

    Parameters
    ----------
    records : list[dict[str, Any]]
        Transformed rows from transform.transform_data(); each dict must
        contain keys: order_id, product, quantity, price, total, date, customer.

    Returns
    -------
    int
        The number of rows successfully upserted into the database.

    Raises
    ------
    ValueError
        If the record list is empty.
    sqlite3.DatabaseError
        On any database-level error (propagated after logging).
    """
    if not records:
        raise ValueError("Load step received an empty record list — nothing to load.")

    db_path = _get_db_path()
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    logger.info(
        "Connecting to SQLite database at '%s'.", db_path
    )

    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Prepare rows with the extra loaded_at column
    rows_to_insert = [
        {**row, "loaded_at": loaded_at} for row in records
    ]

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Ensure the table exists
            cursor.execute(CREATE_TABLE_SQL)
            logger.info("Table 'sales_orders' verified / created.")

            # Bulk upsert inside a single transaction
            cursor.executemany(INSERT_SQL, rows_to_insert)
            conn.commit()

            row_count = cursor.rowcount
            # executemany sets rowcount to the last batch size; query actual count
            cursor.execute("SELECT COUNT(*) FROM sales_orders;")
            total_in_db = cursor.fetchone()[0]

    except sqlite3.DatabaseError as exc:
        logger.exception("Database error during load step: %s", exc)
        raise

    logger.info(
        "Load complete — %d rows upserted | %d total rows now in 'sales_orders'.",
        len(rows_to_insert),
        total_in_db,
    )

    return len(rows_to_insert)
