"""
report.py
---------
Queries the SQLite database and generates a human-readable summary report
saved to data/reports/summary.txt.

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
import sqlite3
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _get_db_path() -> str:
    container_path = "/opt/airflow/database/sales.db"
    if os.path.exists("/opt/airflow"):
        return container_path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    return os.path.join(project_root, "database", "sales.db")


def _get_report_path() -> str:
    container_path = "/opt/airflow/data/reports/summary.txt"
    if os.path.exists("/opt/airflow"):
        return container_path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    return os.path.join(project_root, "data", "reports", "summary.txt")


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def _run_query(conn: sqlite3.Connection, sql: str, params: tuple = ()) -> list[Any]:
    """Execute *sql* with *params* and return all rows."""
    cursor = conn.cursor()
    cursor.execute(sql, params)
    return cursor.fetchall()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_report() -> dict[str, Any]:
    """
    Query the ``sales_orders`` table and produce a summary report file.

    Returns
    -------
    dict[str, Any]
        A dictionary with the key metrics so the DAG can log them via XCom.

    Raises
    ------
    FileNotFoundError
        If the SQLite database does not exist yet (Load step must run first).
    """
    db_path = _get_db_path()

    if not os.path.exists(db_path):
        raise FileNotFoundError(
            f"Database not found at '{db_path}'. Run the Load step first."
        )

    logger.info("Connecting to SQLite database at '%s' for reporting.", db_path)

    with sqlite3.connect(db_path) as conn:
        # ── Core KPIs ────────────────────────────────────────────────────────
        (total_orders,) = _run_query(conn, "SELECT COUNT(*) FROM sales_orders;")[0]
        (total_revenue,) = _run_query(conn, "SELECT ROUND(SUM(total), 2) FROM sales_orders;")[0]
        (avg_order_value,) = _run_query(conn, "SELECT ROUND(AVG(total), 2) FROM sales_orders;")[0]

        # ── Top selling product by units ─────────────────────────────────────
        top_product_rows = _run_query(
            conn,
            """
            SELECT product, SUM(quantity) AS units_sold
            FROM   sales_orders
            GROUP  BY product
            ORDER  BY units_sold DESC
            LIMIT  1;
            """,
        )
        top_product, top_units = top_product_rows[0] if top_product_rows else ("N/A", 0)

        # ── Top 5 products by revenue ────────────────────────────────────────
        top5_products = _run_query(
            conn,
            """
            SELECT   product,
                     ROUND(SUM(total), 2)  AS revenue,
                     COUNT(*)              AS orders
            FROM     sales_orders
            GROUP BY product
            ORDER BY revenue DESC
            LIMIT    5;
            """,
        )

        # ── Revenue by month ─────────────────────────────────────────────────
        monthly_revenue = _run_query(
            conn,
            """
            SELECT   SUBSTR(date, 1, 7)       AS month,
                     ROUND(SUM(total), 2)     AS revenue,
                     COUNT(*)                 AS orders
            FROM     sales_orders
            GROUP BY month
            ORDER BY month ASC;
            """,
        )

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
        "total_revenue":     total_revenue,
        "avg_order_value":   avg_order_value,
        "top_product":       top_product,
        "top_product_units": top_units,
        "report_path":       report_path,
        "generated_at":      generated_at,
    }

    return metrics
