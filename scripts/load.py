"""
load.py
-------
Loads the transformed sales records into a **PostgreSQL** database.

Schema
------
Table: sales_orders
  order_id   TEXT PRIMARY KEY
  product    TEXT         NOT NULL
  quantity   INTEGER      NOT NULL
  price      NUMERIC(12,4) NOT NULL
  total      NUMERIC(12,4) NOT NULL
  date       DATE         NOT NULL
  customer   TEXT         NOT NULL
  loaded_at  TIMESTAMPTZ  NOT NULL   -- UTC timestamp of when the row was inserted

Strategy
--------
  • The table is created with ``CREATE TABLE IF NOT EXISTS`` so repeated
    DAG runs are safe.
  • Each batch uses a PostgreSQL UPSERT:
      INSERT … ON CONFLICT (order_id) DO UPDATE SET …
    This is idempotent: re-running the DAG updates existing rows instead of
    raising duplicate-key errors.
  • A single transaction is used for the entire batch to guarantee
    atomicity: either all rows land, or none do.
  • Connection settings are read from environment variables injected by
    docker-compose (SALES_DB_HOST, SALES_DB_PORT, SALES_DB_NAME,
    SALES_DB_USER, SALES_DB_PASSWORD).
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras   # for execute_values (fast bulk insert)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sales_orders (
    order_id   TEXT             PRIMARY KEY,
    product    TEXT             NOT NULL,
    quantity   INTEGER          NOT NULL,
    price      NUMERIC(12, 4)   NOT NULL,
    total      NUMERIC(12, 4)   NOT NULL,
    date       DATE             NOT NULL,
    customer   TEXT             NOT NULL,
    loaded_at  TIMESTAMPTZ      NOT NULL
);
"""

# PostgreSQL UPSERT — update all columns on primary-key conflict
UPSERT_SQL = """
INSERT INTO sales_orders
    (order_id, product, quantity, price, total, date, customer, loaded_at)
VALUES %s
ON CONFLICT (order_id) DO UPDATE SET
    product   = EXCLUDED.product,
    quantity  = EXCLUDED.quantity,
    price     = EXCLUDED.price,
    total     = EXCLUDED.total,
    date      = EXCLUDED.date,
    customer  = EXCLUDED.customer,
    loaded_at = EXCLUDED.loaded_at;
"""


# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

def _get_connection() -> "psycopg2.connection":
    """
    Return a psycopg2 connection to the sales_db PostgreSQL database.

    Connection parameters are read from environment variables that are
    injected by docker-compose.yaml:

      SALES_DB_HOST     (default: postgres)
      SALES_DB_PORT     (default: 5432)
      SALES_DB_NAME     (default: sales_db)
      SALES_DB_USER     (default: sales_user)
      SALES_DB_PASSWORD (default: sales_password)
    """
    return psycopg2.connect(
        host=os.environ.get("SALES_DB_HOST", "postgres"),
        port=int(os.environ.get("SALES_DB_PORT", 5432)),
        dbname=os.environ.get("SALES_DB_NAME", "sales_db"),
        user=os.environ.get("SALES_DB_USER", "sales_user"),
        password=os.environ.get("SALES_DB_PASSWORD", "sales_password"),
        connect_timeout=10,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def load_to_postgres(records: list[dict[str, Any]]) -> int:
    """
    Insert transformed records into the ``sales_orders`` PostgreSQL table.

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
    psycopg2.DatabaseError
        On any database-level error (propagated after logging).
    """
    if not records:
        raise ValueError("Load step received an empty record list — nothing to load.")

    logger.info(
        "Connecting to PostgreSQL sales_db at %s:%s.",
        os.environ.get("SALES_DB_HOST", "postgres"),
        os.environ.get("SALES_DB_PORT", 5432),
    )

    loaded_at = datetime.now(timezone.utc)

    # Build rows as tuples in the column order of UPSERT_SQL
    rows_to_insert = [
        (
            row["order_id"],
            row["product"],
            row["quantity"],
            row["price"],
            row["total"],
            row["date"],
            row["customer"],
            loaded_at,
        )
        for row in records
    ]

    try:
        conn = _get_connection()
        try:
            with conn:                           # auto-commit / rollback
                with conn.cursor() as cursor:

                    # Ensure the table exists
                    cursor.execute(CREATE_TABLE_SQL)
                    logger.info("Table 'sales_orders' verified / created.")

                    # Bulk UPSERT using execute_values (much faster than executemany)
                    psycopg2.extras.execute_values(
                        cursor, UPSERT_SQL, rows_to_insert
                    )

                    cursor.execute("SELECT COUNT(*) FROM sales_orders;")
                    total_in_db: int = cursor.fetchone()[0]

        finally:
            conn.close()

    except psycopg2.DatabaseError as exc:
        logger.exception("Database error during load step: %s", exc)
        raise

    logger.info(
        "Load complete — %d rows upserted | %d total rows now in 'sales_orders'.",
        len(rows_to_insert),
        total_in_db,
    )

    return len(rows_to_insert)
