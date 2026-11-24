"""Tests for the profiler, cleaning transformations, and analytics queries."""

import sqlite3

import pandas as pd
import pytest

from analytics import QUERIES, run_query
from cleaner import CleaningReport, clean_all, clean_stores, clean_transactions
from loader import build_warehouse
from profiler import profile
from utils import parse_dates, parse_money
from pipeline import ReconciliationError, reconcile

TXN_COLS = ["transaction_id", "transaction_date", "store_id", "product_id",
            "customer_id", "quantity", "unit_price", "total_amount"]


def txns(rows):
    """Build a raw (all-text) transactions DataFrame like read_csv(dtype=str) would."""
    return pd.DataFrame(rows, columns=TXN_COLS, dtype="str")


def test_profile_counts_nulls_duplicates_and_numeric_stats():
    df = pd.DataFrame({
        "id": ["a", "b", "b", None],
        "amount": ["10", "-5", "-5", "0"],
    }, dtype="str")
    result = profile(df, "sample")

    assert result["row_count"] == 4
    assert result["column_count"] == 2
    assert result["duplicate_rows"] == 1
    assert result["columns"]["id"]["null_count"] == 1
    assert result["columns"]["id"]["null_pct"] == 25.0
    num = result["columns"]["amount"]["numeric"]
    assert (num["min"], num["max"]) == (-5.0, 10.0)
    assert num["negative_count"] == 2
    assert num["zero_count"] == 1


def test_profile_empty_dataframe_does_not_crash():
    result = profile(pd.DataFrame({"a": [], "b": []}), "empty")

    assert result["row_count"] == 0
    assert result["duplicate_rows"] == 0
    assert result["columns"]["a"]["null_pct"] == 0.0


def test_profile_all_null_column():
    df = pd.DataFrame({"x": [None, None, None], "y": ["1", "2", "3"]})
    result = profile(df, "nulls")

    assert result["columns"]["x"]["null_count"] == 3
    assert result["columns"]["x"]["null_pct"] == 100.0
    assert "numeric" not in result["columns"]["x"]


def test_profile_date_stats_handle_mixed_formats():
    df = pd.DataFrame({"sale_date": ["2026-03-19", "03/20/2026", "21-03-2026"]})
    stats = profile(df, "dates")["columns"]["sale_date"]["date"]

    assert stats["min_date"] == "2026-03-19"
    assert stats["max_date"] == "2026-03-21"
    assert stats["unparseable_count"] == 0

def test_parse_dates_uses_separator_to_pick_format():
    s = pd.Series(["2026-04-01", "04/01/2026", "01-04-2026", "garbage"])
    out = parse_dates(s)

    assert list(out[:3]) == [pd.Timestamp("2026-04-01")] * 3
    assert pd.isna(out[3])


def test_parse_money_strips_symbols():
    out = parse_money(pd.Series(["$1,234.50", "99", "-12.5", "abc"]))
    assert list(out[:3]) == [1234.50, 99.0, -12.5]
    assert pd.isna(out[3])


def test_clean_transactions_dedupes_flags_and_quarantines():
    raw = txns([
        ["T1", "2026-03-01", "S1", "P1", "C1", "2", "10.00", "$20.00"],
        ["T1", "2026-03-01", "S1", "P1", "C1", "2", "10.00", "$20.00"],
        ["T2", "2026-03-02", "S1", "P1", None, "1", "10.00", "10.00"],
        ["T3", "2026-03-03", "S1", "P1", "C2", "-1", "10.00", "-10.00"],
        ["T4", "2026-03-04", "S1", "P1", "C3", "0", "10.00", "0"],
        ["T5", "2026-03-05", "S1", "P1", "C3", "2", "10.00", "18.00"],
        ["T6", "2099-01-01", "S1", "P1", "C3", "1", "10.00", "10.00"],
    ])
    report = CleaningReport()
    out = clean_transactions(raw, report)

    assert list(out["transaction_id"]) == ["T1", "T2", "T3", "T5"]
    assert out.loc[out.transaction_id == "T1", "total_amount"].item() == 20.0
    assert out.loc[out.transaction_id == "T2", "is_guest"].item()
    assert out.loc[out.transaction_id == "T3", "is_return"].item()
    assert out.loc[out.transaction_id == "T5", "is_price_mismatch"].item()
    assert not out.loc[out.transaction_id == "T1", "is_price_mismatch"].item()

    reasons = sorted(q["reason"] for q in report.quarantine)
    assert len(reasons) == 3
    assert reasons[1] == "Exact duplicate row"
    assert reasons[2] == "Zero quantity (void/test)"
    assert reasons[0].startswith("Dated after extract date")
    dup = next(q for q in report.quarantine if q["reason"] == "Exact duplicate row")
    assert dup["source_row"] == 3
    assert '"$20.00"' in dup["raw_record"]


def test_clean_stores_nulls_bad_zip_and_fills_region_from_census():
    raw = pd.DataFrame({
        "store_id": ["S1", "S2", "S2"],
        "store_name": ["A", "B", "B variant"],
        "city": ["X", "Portland", "Portland"],
        "state": ["NY", "OR", "OR"],
        "zip_code": ["0938", "97220", "97220"],
        "region": ["Northeast", None, None],
        "opened_date": ["2010-01-01"] * 3,
    }, dtype="str")
    out = clean_stores(raw, CleaningReport())

    assert len(out) == 2
    assert pd.isna(out.loc[out.store_id == "S1", "zip_code"].item())
    assert out.loc[out.store_id == "S2", "region"].item() == "West"
    assert out.loc[out.store_id == "S2", "store_name"].item() == "B"


def test_clean_all_keeps_transaction_price_and_adds_unknown_members():
    t = txns([
        ["T1", "2026-03-01", "S1", "P1", "C1", "1", "141.61", "141.61"],
        ["T2", "2026-03-02", "S9", "P9", "C1", "1", "5.00", "5.00"],
    ])
    s = pd.DataFrame({"store_id": ["S1"], "store_name": ["A"], "city": ["X"], "state": ["NY"],
                      "zip_code": ["10001"], "region": ["Northeast"],
                      "opened_date": ["2010-01-01"]}, dtype="str")
    p = pd.DataFrame({"product_id": ["P1", "P1"], "product_name": ["One", "One"],
                      "category": ["A", "A"], "unit_price": ["150.11", "141.61"],
                      "supplier_id": ["X", "X"]}, dtype="str")
    _, stores, products, _ = clean_all(t, s, p)

    assert products.loc[products.product_id == "P1", "list_price"].item() == 141.61
    assert stores.loc[stores.store_id == "S9", "is_inferred"].item()
    assert products.loc[products.product_id == "P9", "is_inferred"].item()

@pytest.fixture
def warehouse(tmp_path):
    """Six hand-picked transactions loaded through the real cleaner + loader.

    Hand-computed answers:
      Q1 window (30 days ending 2026-04-25): S002 = 20 + 100 + 10 = 130; S001 = -100
      Q3 return rate: S001 = 1/3, S002 = 0/3
      Q4 avg (no returns): East = (10 + 100) / 2 = 55; West = (20 + 100 + 10) / 3 = 43.33
      Q5: C2 = 30 (2 orders), C1 = 10 + 100 - 100 = 10 (2 orders, 1 return)
    """
    t = txns([
        ["T1", "2026-03-01", "S001", "P1", "C1", "1", "10", "10"],
        ["T2", "2026-03-15", "S001", "P2", "C1", "1", "100", "$100.00"],
        ["T3", "04/10/2026", "S002", "P1", "C2", "2", "10", "20"],
        ["T4", "20-04-2026", "S002", "P2", None, "1", "100", "100"],
        ["T5", "2026-04-25", "S001", "P2", "C1", "-1", "100", "-100"],
        ["T6", "2026-04-01", "S002", "P1", "C2", "1", "10", "10"],
    ])
    s = pd.DataFrame({
        "store_id": ["S001", "S002"], "store_name": ["East One", "West One"],
        "city": ["A", "B"], "state": ["NY", "WA"], "zip_code": ["10001", "98001"],
        "region": ["East", "West"], "opened_date": ["2010-01-01", "2010-01-01"],
    }, dtype="str")
    p = pd.DataFrame({
        "product_id": ["P1", "P2"], "product_name": ["One", "Two"], "category": ["A", "B"],
        "unit_price": ["10", "100"], "supplier_id": ["X", "X"],
    }, dtype="str")
    t, s, p, report = clean_all(t, s, p)
    db = tmp_path / "test.db"
    build_warehouse(t, s, p, db, quarantine=report.quarantine_df())
    conn = sqlite3.connect(db)
    yield conn
    conn.close()


def test_top_stores_uses_net_revenue_in_last_30_days(warehouse):
    rows = run_query(warehouse, QUERIES["q1_top_5_stores_last_30_days"])

    assert [(r["store_id"], r["net_revenue"]) for r in rows] == [("S002", 130.0),
                                                                 ("S001", -100.0)]
    assert rows[0]["window_start"] == "2026-03-27"
    assert rows[0]["window_end"] == "2026-04-25"


def test_return_rate_and_flag(warehouse):
    rows = {r["store_id"]: r for r in run_query(warehouse, QUERIES["q3_return_rate_by_store"])}

    assert rows["S001"]["return_rate"] == pytest.approx(1 / 3, abs=1e-4)
    assert rows["S001"]["high_return_flag"] == 1
    assert rows["S002"]["return_rate"] == 0
    assert rows["S002"]["high_return_flag"] == 0


def test_avg_transaction_value_excludes_returns(warehouse):
    rows = {r["region"]: r for r in
            run_query(warehouse, QUERIES["q4_avg_transaction_value_by_region"])}

    assert rows["East"]["avg_transaction_value"] == 55.0
    assert rows["West"]["avg_transaction_value"] == 43.33


def test_top_customers_exclude_guests_and_net_returns(warehouse):
    rows = run_query(warehouse, QUERIES["q5_top_10_customers"])

    assert [r["customer_id"] for r in rows] == ["C2", "C1"]
    c1 = rows[1]
    assert (c1["lifetime_spend"], c1["transaction_count"], c1["return_count"]) == (10.0, 2, 1)
    assert c1["avg_order_value"] == 55.0


def test_mom_change_by_category(warehouse):
    rows = {(r["category"], r["year_month"]): r
            for r in run_query(warehouse, QUERIES["q2_mom_revenue_change_by_category"])}

    assert rows[("A", "2026-04")]["mom_change_pct"] == 200.0
    assert rows[("B", "2026-04")]["mom_change_pct"] == -100.0
    assert rows[("A", "2026-03")]["mom_change_pct"] is None

def test_reconcile_accounts_for_every_raw_row():
    raw_t = txns([
        ["T1", "2026-03-01", "S1", "P1", "C1", "1", "10", "10"],
        ["T1", "2026-03-01", "S1", "P1", "C1", "1", "10", "10"],
        ["T2", "2026-03-02", "S9", "P1", "C1", "1", "10", "10"],
    ])
    raw_s = pd.DataFrame({"store_id": ["S1"], "store_name": ["A"], "city": ["X"],
                          "state": ["NY"], "zip_code": ["10001"], "region": ["Northeast"],
                          "opened_date": ["2010-01-01"]}, dtype="str")
    raw_p = pd.DataFrame({"product_id": ["P1"], "product_name": ["One"], "category": ["A"],
                          "unit_price": ["10"], "supplier_id": ["X"]}, dtype="str")
    t, s, p, report = clean_all(raw_t, raw_s, raw_p)

    result = reconcile({"transactions": raw_t, "stores": raw_s, "products": raw_p},
                       {"transactions": t, "stores": s, "products": p}, report)

    assert result["transactions"] == {"raw": 3, "kept": 2, "quarantined": 1}
    assert result["stores"] == {"raw": 1, "kept": 1, "quarantined": 0}


def test_reconcile_raises_when_a_row_goes_missing():
    raw_t = txns([
        ["T1", "2026-03-01", "S1", "P1", "C1", "1", "10", "10"],
        ["T2", "2026-03-02", "S1", "P1", "C1", "1", "10", "10"],
    ])
    lost_one = raw_t.iloc[:1].assign(is_inferred=False)  # simulate a silent drop

    with pytest.raises(ReconciliationError, match="transactions: raw 2 != kept 1"):
        reconcile({"transactions": raw_t}, {"transactions": lost_one}, CleaningReport())