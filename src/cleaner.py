"""Cleaning pipeline: turn raw CSV DataFrames into clean, validated DataFrames.

Every fix appends an entry to an issue log, so the README table and the
code can't drift apart. Every row that is removed goes to a quarantine
list with its original raw values and a reason, so nothing disappears silently.
"""

import json
from dataclasses import dataclass, field

import pandas as pd

from utils import parse_dates, parse_money

UNKNOWN = "Unknown"

EXTRACT_DATE = pd.Timestamp("2026-06-02")


CENSUS_REGION = {
    **dict.fromkeys(["CT", "ME", "MA", "NH", "RI", "VT", "NJ", "NY", "PA"], "Northeast"),
    **dict.fromkeys(["IL", "IN", "MI", "OH", "WI", "IA", "KS", "MN", "MO", "NE",
                     "ND", "SD"], "Midwest"),
    **dict.fromkeys(["DE", "DC", "FL", "GA", "MD", "NC", "SC", "VA", "WV", "AL",
                     "KY", "MS", "TN", "AR", "LA", "OK", "TX"], "South"),
    **dict.fromkeys(["AZ", "CO", "ID", "MT", "NV", "NM", "UT", "WY", "AK", "CA",
                     "HI", "OR", "WA"], "West"),
}


@dataclass
class CleaningReport:
    issues: list = field(default_factory=list)
    quarantine: list = field(default_factory=list)

    def log(self, issue: str, file: str, count: int, decision: str) -> None:
        self.issues.append({"issue": issue, "file": file, "count": int(count),
                            "decision": decision})

    def quarantine_rows(self, raw: pd.DataFrame, file: str, reason: str) -> None:
        """Keep removed rows with their original raw values.

        `raw` must keep read_csv's index, so source_row = index + 2 (header is line 1).
        """
        for idx, row in raw.iterrows():
            self.quarantine.append({
                "source_file": file,
                "source_row": int(idx) + 2,
                "reason": reason,
                "raw_record": json.dumps(
                    {k: (None if pd.isna(v) else v) for k, v in row.items()}),
            })

    def quarantine_df(self) -> pd.DataFrame:
        return pd.DataFrame(self.quarantine,
                            columns=["source_file", "source_row", "reason", "raw_record"])


def clean_transactions(df: pd.DataFrame, report: CleaningReport) -> pd.DataFrame:
    raw = df.copy()
    df = df.copy()
    f = "transactions"

    dup = df.duplicated()
    n = dup.sum()
    report.quarantine_rows(raw[dup], f, "Exact duplicate row")
    df = df[~dup]
    report.log("Exact duplicate rows", f, n, "Dropped, kept first")

    dup_id = df["transaction_id"].duplicated()
    n = dup_id.sum()
    if n:
        report.quarantine_rows(raw.loc[df.index[dup_id]], f, "Duplicate transaction_id")
        df = df[~dup_id]
        report.log("Duplicate transaction_id with differing data", f, n, "Kept first")

    non_iso = (~df["transaction_date"].str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)).sum()
    df["transaction_date"] = parse_dates(df["transaction_date"])
    report.log("Non-ISO date formats (MM/DD/YYYY, DD-MM-YYYY)", f, non_iso,
               "Parsed each format explicitly")

    bad_date = df["transaction_date"].isna()
    n = bad_date.sum()
    if n:
        report.quarantine_rows(raw.loc[df.index[bad_date]], f, "Unparseable transaction_date")
        df = df[~bad_date]
        report.log("Unparseable transaction_date", f, n, "Dropped")

    future = df["transaction_date"] > EXTRACT_DATE
    n = future.sum()
    if n:
        report.quarantine_rows(raw.loc[df.index[future]], f,
                               f"Dated after extract date {EXTRACT_DATE.date()}")
        df = df[~future]
        report.log(f"Future-dated (after extract date {EXTRACT_DATE.date()})", f, n,
                   "Quarantined")

    n = df["total_amount"].str.contains(r"\$", na=False).sum()
    df["total_amount"] = parse_money(df["total_amount"])
    df["unit_price"] = parse_money(df["unit_price"])
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
    report.log("'$' prefix in total_amount", f, n, "Stripped and converted to number")

    zero = (df["quantity"] == 0).fillna(False)
    report.quarantine_rows(raw.loc[df.index[zero]], f, "Zero quantity (void/test)")
    report.log("Zero quantity / $0 total (void or test)", f, zero.sum(), "Excluded")
    df = df[~zero]

    expected = (df["quantity"] * df["unit_price"]).round(2)
    df["is_price_mismatch"] = ((df["total_amount"] - expected).abs() > 0.01).fillna(False)
    report.log("total_amount != quantity x unit_price (discount)", f,
               df["is_price_mismatch"].sum(),
               "Kept total_amount as charged, flagged is_price_mismatch")

    df["customer_id"] = df["customer_id"].str.strip()
    df["is_guest"] = df["customer_id"].isna()
    report.log("Null customer_id (guest checkout)", f, df["is_guest"].sum(),
               "Kept, flagged is_guest")

    df["is_return"] = df["quantity"] < 0
    report.log("Negative quantity (return)", f, df["is_return"].sum(),
               "Kept, flagged is_return")

    return df.reset_index(drop=True)


def clean_stores(df: pd.DataFrame, report: CleaningReport) -> pd.DataFrame:
    raw = df.copy()
    df = df.copy()
    f = "stores"
    for col in df.columns:  # all columns are read as text
        df[col] = df[col].str.strip()

    dup = df["store_id"].duplicated()
    n = dup.sum()
    report.quarantine_rows(raw[dup], f, "Duplicate store_id (name variant)")
    df = df[~dup]
    report.log("Duplicate store_id (name variant)", f, n, "Kept first row")

    bad_zip = ~df["zip_code"].str.fullmatch(r"\d{5}", na=False)
    report.log("Malformed zip_code (not 5 digits)", f, bad_zip.sum(),
               "Set to NULL; padding would invent a wrong zip")
    df.loc[bad_zip, "zip_code"] = None

    missing = df["region"].isna()
    df.loc[missing, "region"] = df.loc[missing, "state"].map(CENSUS_REGION)
    still = df["region"].isna().sum()
    df["region"] = df["region"].fillna(UNKNOWN)
    report.log("Null region", f, missing.sum(),
               f"Filled from US Census region of state ({missing.sum() - still}); "
               f"else '{UNKNOWN}' ({still})")

    df["opened_date"] = parse_dates(df["opened_date"])
    df["is_inferred"] = False
    return df.reset_index(drop=True)


def clean_products(df: pd.DataFrame, transactions: pd.DataFrame,
                   report: CleaningReport) -> pd.DataFrame:
    """`transactions` must already be cleaned (numeric unit_price)."""
    raw = df.copy()
    df = df.copy()
    f = "products"
    df["unit_price"] = parse_money(df["unit_price"])

    dup = df.duplicated()
    n = dup.sum()
    report.quarantine_rows(raw[dup], f, "Exact duplicate row")
    df = df[~dup]
    report.log("Exact duplicate rows", f, n, "Dropped")

    # Most common price actually charged per product.
    txn_price = transactions.groupby("product_id")["unit_price"].agg(
        lambda x: x.mode().iloc[0] if not x.mode().empty else None
    )

    multi = df["product_id"].duplicated(keep=False)
    n = df.loc[multi, "product_id"].nunique()
    if n:
        df["_matches"] = df["unit_price"] == df["product_id"].map(txn_price)
        df = df.sort_values("_matches", ascending=False, kind="stable")
        stale = df["product_id"].duplicated()
        report.quarantine_rows(raw.loc[df.index[stale]], f,
                               "Second price not used by any transaction")
        df = df[~stale].drop(columns="_matches")
        report.log("Product with more than one price", f, n,
                   "Kept the price transactions actually use")

    zero = df["unit_price"] == 0
    df.loc[zero, "unit_price"] = df.loc[zero, "product_id"].map(txn_price)
    report.log("Zero unit_price", f, zero.sum(), "Replaced with most common transaction price")

    n = df["category"].isna().sum()
    df["category"] = df["category"].fillna(UNKNOWN)
    report.log("Null category", f, n, f"Filled with '{UNKNOWN}'")

    df = df.rename(columns={"unit_price": "list_price"})
    df["is_inferred"] = False
    return df.sort_values("product_id").reset_index(drop=True)


def add_unknown_members(transactions, stores, products, report):
    """Add placeholder dimension rows for IDs that transactions reference but
    the reference files don't contain, so no revenue is silently dropped."""
    missing_s = sorted(set(transactions["store_id"]) - set(stores["store_id"]))
    n = transactions["store_id"].isin(missing_s).sum()
    if missing_s:
        stores = pd.concat([stores, pd.DataFrame({
            "store_id": missing_s, "store_name": UNKNOWN, "city": UNKNOWN,
            "state": UNKNOWN, "zip_code": None, "region": UNKNOWN,
            "opened_date": pd.NaT, "is_inferred": True,
        })], ignore_index=True)
        report.log(f"Unknown store_id ({', '.join(missing_s)})", "transactions", n,
                   "Kept; added placeholder dim_store rows")

    missing_p = sorted(set(transactions["product_id"]) - set(products["product_id"]))
    n = transactions["product_id"].isin(missing_p).sum()
    if missing_p:
        products = pd.concat([products, pd.DataFrame({
            "product_id": missing_p, "product_name": UNKNOWN, "category": UNKNOWN,
            "list_price": None, "supplier_id": None, "is_inferred": True,
        })], ignore_index=True)
        report.log(f"Unknown product_id ({', '.join(missing_p)})", "transactions", n,
                   "Kept; added placeholder dim_product rows")

    return stores, products


def clean_all(transactions, stores, products):
    """Clean all three raw DataFrames. Returns (transactions, stores, products, report)."""
    report = CleaningReport()
    t = clean_transactions(transactions, report)
    s = clean_stores(stores, report)
    p = clean_products(products, t, report)
    s, p = add_unknown_members(t, s, p, report)
    return t, s, p, report


if __name__ == "__main__":
    from pathlib import Path

    raw = Path(__file__).resolve().parent.parent / "data" / "raw"
    t, s, p, report = clean_all(
        pd.read_csv(raw / "transactions.csv", dtype=str),
        pd.read_csv(raw / "stores.csv", dtype=str),
        pd.read_csv(raw / "products.csv", dtype=str),
    )
    print(pd.DataFrame(report.issues).to_string(index=False))
    print(f"\nquarantined rows: {len(report.quarantine)}")
    print(report.quarantine_df().groupby(["source_file", "reason"]).size().to_string())
    print(f"\ntransactions: {len(t)}  stores: {len(s)}  products: {len(p)}")
    print(f"date range: {t['transaction_date'].min().date()} to {t['transaction_date'].max().date()}")