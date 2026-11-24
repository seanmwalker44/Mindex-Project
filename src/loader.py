"""Load cleaned DataFrames into a SQLite star schema."""

import sqlite3
from pathlib import Path

import pandas as pd

SCHEMA = """
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS quarantine;

CREATE TABLE dim_date (
    date_key      INTEGER PRIMARY KEY,      -- YYYYMMDD
    full_date     TEXT    NOT NULL UNIQUE,  -- ISO 'YYYY-MM-DD'
    year          INTEGER NOT NULL,
    quarter       INTEGER NOT NULL,
    month         INTEGER NOT NULL,
    month_name    TEXT    NOT NULL,
    year_month    TEXT    NOT NULL,         -- 'YYYY-MM', handy for MoM queries
    day           INTEGER NOT NULL,
    day_of_week   INTEGER NOT NULL,         -- 0 = Monday
    day_name      TEXT    NOT NULL,
    week_of_year  INTEGER NOT NULL,
    is_weekend    INTEGER NOT NULL
);

CREATE TABLE dim_store (
    store_key     INTEGER PRIMARY KEY,
    store_id      TEXT    NOT NULL UNIQUE,
    store_name    TEXT    NOT NULL,
    city          TEXT,
    state         TEXT,
    zip_code      TEXT,                     -- text; NULL if malformed in source
    region        TEXT    NOT NULL,
    opened_date   TEXT,
    is_inferred   INTEGER NOT NULL          -- 1 = placeholder for an ID missing from stores.csv
);

CREATE TABLE dim_product (
    product_key   INTEGER PRIMARY KEY,
    product_id    TEXT    NOT NULL UNIQUE,
    product_name  TEXT    NOT NULL,
    category      TEXT    NOT NULL,
    list_price    REAL,                     -- reference price; revenue uses fact_sales.unit_price
    supplier_id   TEXT,
    is_inferred   INTEGER NOT NULL
);

CREATE TABLE fact_sales (
    transaction_id    TEXT    PRIMARY KEY,
    date_key          INTEGER NOT NULL REFERENCES dim_date(date_key),
    store_key         INTEGER NOT NULL REFERENCES dim_store(store_key),
    product_key       INTEGER NOT NULL REFERENCES dim_product(product_key),
    customer_id       TEXT,                 -- NULL = guest
    quantity          INTEGER NOT NULL,     -- negative = return
    unit_price        REAL    NOT NULL,     -- price actually charged
    total_amount      REAL    NOT NULL,     -- negative for returns
    is_return         INTEGER NOT NULL,
    is_guest          INTEGER NOT NULL,
    is_price_mismatch INTEGER NOT NULL      -- total_amount != quantity x unit_price
);

-- Rows removed during cleaning, kept for audit. Not part of the star schema.
CREATE TABLE quarantine (
    quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file   TEXT    NOT NULL,
    source_row    INTEGER NOT NULL,         -- line number in the raw CSV
    reason        TEXT    NOT NULL,
    raw_record    TEXT    NOT NULL          -- original values as JSON
);

CREATE INDEX idx_fact_date    ON fact_sales(date_key);
CREATE INDEX idx_fact_store   ON fact_sales(store_key);
CREATE INDEX idx_fact_product ON fact_sales(product_key);
"""


def _to_records(df: pd.DataFrame) -> list[tuple]:
    """Convert a DataFrame to tuples SQLite understands (NaN/NA -> None, bool -> int)."""
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == bool or str(out[col].dtype) == "boolean":
            out[col] = out[col].astype("Int64")
    out = out.astype(object).where(out.notna(), None)
    return [tuple(row) for row in out.itertuples(index=False)]


def _insert(conn: sqlite3.Connection, table: str, df: pd.DataFrame) -> None:
    cols = ", ".join(df.columns)
    marks = ", ".join("?" for _ in df.columns)
    conn.executemany(f"INSERT INTO {table} ({cols}) VALUES ({marks})", _to_records(df))


def build_dim_date(transactions: pd.DataFrame) -> pd.DataFrame:
    """One row per calendar day from the first to the last transaction date."""
    dates = pd.date_range(transactions["transaction_date"].min(),
                          transactions["transaction_date"].max(), freq="D")
    return pd.DataFrame({
        "date_key": dates.strftime("%Y%m%d").astype(int),
        "full_date": dates.strftime("%Y-%m-%d"),
        "year": dates.year,
        "quarter": dates.quarter,
        "month": dates.month,
        "month_name": dates.strftime("%B"),
        "year_month": dates.strftime("%Y-%m"),
        "day": dates.day,
        "day_of_week": dates.day_of_week,
        "day_name": dates.strftime("%A"),
        "week_of_year": dates.isocalendar().week.astype(int).values,
        "is_weekend": (dates.day_of_week >= 5).astype(int),
    })


def build_warehouse(transactions: pd.DataFrame, stores: pd.DataFrame,
                    products: pd.DataFrame, db_path: Path,
                    quarantine: pd.DataFrame | None = None) -> dict:
    """Create the star schema at db_path and load it. Returns row counts per table."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)


    stores = stores.sort_values("store_id").reset_index(drop=True)
    stores.insert(0, "store_key", range(1, len(stores) + 1))
    stores["opened_date"] = stores["opened_date"].dt.strftime("%Y-%m-%d")

    products = products.sort_values("product_id").reset_index(drop=True)
    products.insert(0, "product_key", range(1, len(products) + 1))

    dim_date = build_dim_date(transactions)

    fact = (transactions
            .merge(stores[["store_id", "store_key"]], on="store_id", how="left")
            .merge(products[["product_id", "product_key"]], on="product_id", how="left"))
    fact["date_key"] = fact["transaction_date"].dt.strftime("%Y%m%d").astype(int)

    missing = fact[["store_key", "product_key"]].isna().sum().sum()
    if missing:
        raise ValueError(f"{missing} fact rows have no matching dimension key")

    fact = fact[["transaction_id", "date_key", "store_key", "product_key", "customer_id",
                 "quantity", "unit_price", "total_amount", "is_return", "is_guest",
                 "is_price_mismatch"]]

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(SCHEMA)
        _insert(conn, "dim_date", dim_date)
        _insert(conn, "dim_store", stores[[
            "store_key", "store_id", "store_name", "city", "state", "zip_code",
            "region", "opened_date", "is_inferred"]])
        _insert(conn, "dim_product", products[[
            "product_key", "product_id", "product_name", "category", "list_price",
            "supplier_id", "is_inferred"]])
        _insert(conn, "fact_sales", fact)
        if quarantine is not None and len(quarantine):
            _insert(conn, "quarantine", quarantine[
                ["source_file", "source_row", "reason", "raw_record"]])

        counts = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                  for t in ["dim_date", "dim_store", "dim_product", "fact_sales", "quarantine"]}
    return counts


if __name__ == "__main__":
    from cleaner import clean_all

    root = Path(__file__).resolve().parent.parent
    raw = root / "data" / "raw"
    t, s, p, report = clean_all(
        pd.read_csv(raw / "transactions.csv", dtype=str),
        pd.read_csv(raw / "stores.csv", dtype=str),
        pd.read_csv(raw / "products.csv", dtype=str),
    )
    counts = build_warehouse(t, s, p, root / "output" / "warehouse.db",
                             quarantine=report.quarantine_df())
    for table, n in counts.items():
        print(f"{table:12} {n:>5} rows")