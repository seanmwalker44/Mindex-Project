"""Business questions answered in SQL against the warehouse.

SQL rather than pandas: the data already lives in a star schema, the queries
read like the questions, and they run unchanged on any SQL warehouse.
Revenue is always net: returns are negative total_amount and reduce it.
"""

import json
import sqlite3
from pathlib import Path

RETURN_RATE_THRESHOLD = 0.10

# Query 1: "most recent 30 days" is anchored on the last date in the data, not today.
TOP_STORES_LAST_30_DAYS = """
WITH bounds AS (
    SELECT MAX(d.full_date)                 AS end_date,
           DATE(MAX(d.full_date), '-29 days') AS start_date
    FROM fact_sales f JOIN dim_date d ON d.date_key = f.date_key
)
SELECT s.store_id,
       s.store_name,
       s.region,
       ROUND(SUM(f.total_amount), 2) AS net_revenue,
       COUNT(*)                      AS transactions,
       b.start_date                  AS window_start,
       b.end_date                    AS window_end
FROM fact_sales f
JOIN dim_date  d ON d.date_key  = f.date_key
JOIN dim_store s ON s.store_key = f.store_key
CROSS JOIN bounds b
WHERE d.full_date BETWEEN b.start_date AND b.end_date
GROUP BY s.store_key
ORDER BY net_revenue DESC
LIMIT 5;
"""

# Query 2: every category x every month, so a month with no sales counts as 0
# instead of being skipped by LAG().
MOM_REVENUE_BY_CATEGORY = """
WITH months AS (
    SELECT year_month,
           COUNT(*) AS days_in_data,
           CASE WHEN COUNT(*) < 28 THEN 1 ELSE 0 END AS is_partial_month
    FROM dim_date
    GROUP BY year_month
),
categories AS (
    SELECT DISTINCT category FROM dim_product
),
monthly AS (
    SELECT p.category, d.year_month, SUM(f.total_amount) AS revenue
    FROM fact_sales f
    JOIN dim_date    d ON d.date_key    = f.date_key
    JOIN dim_product p ON p.product_key = f.product_key
    GROUP BY p.category, d.year_month
),
grid AS (
    SELECT c.category, m.year_month, m.days_in_data, m.is_partial_month,
           COALESCE(mo.revenue, 0) AS revenue
    FROM categories c
    CROSS JOIN months m
    LEFT JOIN monthly mo ON mo.category = c.category AND mo.year_month = m.year_month
)
SELECT category,
       year_month,
       days_in_data,
       is_partial_month,
       ROUND(revenue, 2) AS net_revenue,
       ROUND(LAG(revenue) OVER w, 2) AS prev_month_revenue,
       CASE WHEN LAG(revenue) OVER w IS NULL OR LAG(revenue) OVER w = 0 THEN NULL
            ELSE ROUND((revenue - LAG(revenue) OVER w) * 100.0 / ABS(LAG(revenue) OVER w), 2)
       END AS mom_change_pct
FROM grid
WINDOW w AS (PARTITION BY category ORDER BY year_month)
ORDER BY category, year_month;
"""

# Query 3: return transactions / all transactions, per store.
RETURN_RATE_BY_STORE = f"""
SELECT s.store_id,
       s.store_name,
       SUM(f.is_return)                           AS return_transactions,
       COUNT(*)                                   AS total_transactions,
       ROUND(SUM(f.is_return) * 1.0 / COUNT(*), 4) AS return_rate,
       CASE WHEN SUM(f.is_return) * 1.0 / COUNT(*) > {RETURN_RATE_THRESHOLD}
            THEN 1 ELSE 0 END                     AS high_return_flag
FROM fact_sales f
JOIN dim_store s ON s.store_key = f.store_key
GROUP BY s.store_key
ORDER BY return_rate DESC, s.store_id;
"""

# Query 4: returns excluded entirely.
AVG_TRANSACTION_VALUE_BY_REGION = """
SELECT s.region,
       ROUND(AVG(f.total_amount), 2) AS avg_transaction_value,
       COUNT(*)                      AS transactions
FROM fact_sales f
JOIN dim_store s ON s.store_key = f.store_key
WHERE f.is_return = 0
GROUP BY s.region
ORDER BY avg_transaction_value DESC;
"""

# Query5: guestshoppers excluded. Lifetime spend is net of returns; order count and
# average order value use purchases only, so a return isn't counted as an order.
TOP_CUSTOMERS = """
SELECT customer_id,
       ROUND(SUM(total_amount), 2)                                     AS lifetime_spend,
       SUM(CASE WHEN is_return = 0 THEN 1 ELSE 0 END)                  AS transaction_count,
       ROUND(AVG(CASE WHEN is_return = 0 THEN total_amount END), 2)    AS avg_order_value,
       SUM(is_return)                                                  AS return_count
FROM fact_sales
WHERE is_guest = 0
GROUP BY customer_id
ORDER BY lifetime_spend DESC, customer_id
LIMIT 10;
"""

QUERIES = {
    "q1_top_5_stores_last_30_days": TOP_STORES_LAST_30_DAYS,
    "q2_mom_revenue_change_by_category": MOM_REVENUE_BY_CATEGORY,
    "q3_return_rate_by_store": RETURN_RATE_BY_STORE,
    "q4_avg_transaction_value_by_region": AVG_TRANSACTION_VALUE_BY_REGION,
    "q5_top_10_customers": TOP_CUSTOMERS,
}


def run_query(conn: sqlite3.Connection, sql: str) -> list[dict]:
    """Run a query and return rows as a list of dicts."""
    cur = conn.execute(sql)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def run_all(conn: sqlite3.Connection) -> dict:
    return {name: run_query(conn, sql) for name, sql in QUERIES.items()}


def write_analytics(db_path: Path, out_path: Path) -> dict:
    with sqlite3.connect(db_path) as conn:
        results = run_all(conn)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    return results


if __name__ == "__main__":
    root = Path(__file__).resolve().parent.parent
    results = write_analytics(root / "output" / "warehouse.db",
                              root / "output" / "analytics.json")
    for name, rows in results.items():
        print(f"\n== {name} ({len(rows)} rows)")
        for row in rows[:5]:
            print("  ", row)