"""
report.py
---------
Queries the **PostgreSQL** ``sales_db`` database and generates a
human-readable summary report saved to data/reports/summary.txt.

Report sections
---------------
  ┌─────────────────────────────────────────────────────────┐
  │  SALES SUMMARY REPORT                                   │
  │  Generated: <ISO-8601 timestamp>                        │
  ├─────────────────────────────────────────────────────────┤
  │  Total Orders          : <n>                            │
  │  Total Revenue         : ₹<amount>                      │
  │  Average Order Value   : ₹<amount>                      │
  │  Top Selling Product   : <product> (<n> units sold)     │
  ├─────────────────────────────────────────────────────────┤
  │  Top 5 Products by Revenue                              │
  │  ─────────────────────────────────────────────────────  │
  │  1. <product>  ₹<revenue>  (<n> orders)                 │
  │  ...                                                    │
  ├─────────────────────────────────────────────────────────┤
  │  Revenue by Month                                       │
  │  ...                                                    │
  └─────────────────────────────────────────────────────────┘
"""

import logging
import os
from datetime import datetime, timezone
from typing import Any

import psycopg2

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Path helper (report file — still written to disk for artefact purposes)
# ---------------------------------------------------------------------------

def _get_report_path() -> str:
    container_path = "/opt/airflow/data/reports/summary.txt"
    if os.path.exists("/opt/airflow"):
        return container_path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    return os.path.join(project_root, "data", "reports", "summary.txt")


# ---------------------------------------------------------------------------
# Connection helper (mirrors load.py)
# ---------------------------------------------------------------------------

def _get_connection() -> "psycopg2.connection":
    """Return a psycopg2 connection using SALES_DB_* env vars."""
    return psycopg2.connect(
        host=os.environ.get("SALES_DB_HOST", "postgres"),
        port=int(os.environ.get("SALES_DB_PORT", 5432)),
        dbname=os.environ.get("SALES_DB_NAME", "sales_db"),
        user=os.environ.get("SALES_DB_USER", "sales_user"),
        password=os.environ.get("SALES_DB_PASSWORD", "sales_password"),
        connect_timeout=10,
    )


# ---------------------------------------------------------------------------
# Query helper
# ---------------------------------------------------------------------------

def _run_query(cursor: Any, sql: str, params: tuple = ()) -> list[Any]:
    """Execute *sql* with *params* and return all rows."""
    cursor.execute(sql, params)
    return cursor.fetchall()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_report() -> dict[str, Any]:
    """
    Query the ``sales_orders`` table in PostgreSQL and produce a summary
    report file.

    Returns
    -------
    dict[str, Any]
        A dictionary with the key metrics so the DAG can log them via XCom.

    Raises
    ------
    psycopg2.DatabaseError
        On any database-level error (propagated after logging).
    """
    logger.info(
        "Connecting to PostgreSQL sales_db at %s:%s for reporting.",
        os.environ.get("SALES_DB_HOST", "postgres"),
        os.environ.get("SALES_DB_PORT", 5432),
    )

    try:
        conn = _get_connection()
        try:
            with conn.cursor() as cursor:

                # ── Core KPIs ─────────────────────────────────────────────
                _run_query(cursor, "SELECT COUNT(*) FROM sales_orders;")
                (total_orders,) = cursor.fetchone() if False else _run_query(
                    cursor, "SELECT COUNT(*) FROM sales_orders;"
                )[0]

                (total_revenue,) = _run_query(
                    cursor,
                    "SELECT ROUND(SUM(total)::numeric, 2) FROM sales_orders;"
                )[0]

                (avg_order_value,) = _run_query(
                    cursor,
                    "SELECT ROUND(AVG(total)::numeric, 2) FROM sales_orders;"
                )[0]

                # ── Top selling product by units ───────────────────────────
                top_product_rows = _run_query(
                    cursor,
                    """
                    SELECT product, SUM(quantity) AS units_sold
                    FROM   sales_orders
                    GROUP  BY product
                    ORDER  BY units_sold DESC
                    LIMIT  1;
                    """,
                )
                top_product, top_units = (
                    top_product_rows[0] if top_product_rows else ("N/A", 0)
                )

                # ── Top 5 products by revenue ──────────────────────────────
                top5_products = _run_query(
                    cursor,
                    """
                    SELECT   product,
                             ROUND(SUM(total)::numeric, 2) AS revenue,
                             COUNT(*)                      AS orders
                    FROM     sales_orders
                    GROUP BY product
                    ORDER BY revenue DESC
                    LIMIT    5;
                    """,
                )

                # ── Revenue by month ───────────────────────────────────────
                # Use TO_CHAR on the DATE column — cleaner than SUBSTR on TEXT
                monthly_revenue = _run_query(
                    cursor,
                    """
                    SELECT   TO_CHAR(date, 'YYYY-MM')         AS month,
                             ROUND(SUM(total)::numeric, 2)    AS revenue,
                             COUNT(*)                         AS orders
                    FROM     sales_orders
                    GROUP BY month
                    ORDER BY month ASC;
                    """,
                )

        finally:
            conn.close()

    except psycopg2.DatabaseError as exc:
        logger.exception("Database error during report step: %s", exc)
        raise

    # ── Build report text ────────────────────────────────────────────────────
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    divider      = "─" * 60

    lines = [
        "╔" + "═" * 60 + "╗",
        "║{:^60}║".format("  SALES SUMMARY REPORT  "),
        "╠" + "═" * 60 + "╣",
        "║  Generated : {:<46}║".format(generated_at),
        "╠" + "═" * 60 + "╣",
        "║{:<60}║".format("  KEY METRICS"),
        "║  " + divider + "  ║"[:-2],
        "║  Total Orders          : {:<34}║".format(total_orders),
        "║  Total Revenue         : ₹{:<33}║".format(f"{total_revenue:,.2f}"),
        "║  Average Order Value   : ₹{:<33}║".format(f"{avg_order_value:,.2f}"),
        "║  Top Selling Product   : {:<34}║".format(
            f"{top_product} ({top_units:,} units sold)"
        ),
        "╠" + "═" * 60 + "╣",
        "║{:<60}║".format("  TOP 5 PRODUCTS BY REVENUE"),
        "║  " + divider + "  ║"[:-2],
    ]

    for rank, (product, revenue, orders) in enumerate(top5_products, start=1):
        lines.append(
            "║  {:>2}. {:<30} ₹{:>12,.2f}  ({:>2} orders)║".format(
                rank, product, revenue, orders
            )
        )

    lines += [
        "╠" + "═" * 60 + "╣",
        "║{:<60}║".format("  MONTHLY REVENUE BREAKDOWN"),
        "║  " + divider + "  ║"[:-2],
    ]

    for month, revenue, orders in monthly_revenue:
        lines.append(
            "║  {} ── ₹{:>12,.2f}  ({:>2} orders){:<15}║".format(
                month, revenue, orders, ""
            )
        )

    lines += ["╚" + "═" * 60 + "╝"]

    report_text = "\n".join(lines)

    # ── Write to file ────────────────────────────────────────────────────────
    report_path = _get_report_path()
    os.makedirs(os.path.dirname(report_path), exist_ok=True)

    with open(report_path, "w", encoding="utf-8") as fh:
        fh.write(report_text + "\n")

    logger.info("Summary report written to '%s'.", report_path)
    logger.info("\n%s", report_text)   # Echo to Airflow task logs

    metrics = {
        "total_orders":      total_orders,
        "total_revenue":     float(total_revenue),
        "avg_order_value":   float(avg_order_value),
        "top_product":       top_product,
        "top_product_units": top_units,
        "report_path":       report_path,
        "generated_at":      generated_at,
    }

    return metrics
