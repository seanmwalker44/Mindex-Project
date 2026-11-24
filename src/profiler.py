import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
OUTPUT_DIR = ROOT / "output"
NUMERIC_THRESHOLD = 0.8

def numeric_profile(s: pd.Series) -> dict | None:
    """Returns numeric profile stats if the column is numeric, else None"""
    nums = pd.to_numeric(s, errors="coerce")
    valid=nums.notna().sum()
    if valid == 0 or valid < NUMERIC_THRESHOLD * s.notna().sum():
        return None
    return {
        "min": float(nums.min()),
        "max": float(nums.max()),
        "mean": round(float(nums.mean()), 2),
        "zero_count": int((nums == 0).sum()),
        "negative_count": int((nums < 0).sum()),
        "non_numeric_count": int(s.notna().sum() - valid),
    }

def date_profile(s: pd.Series) -> dict | None:
    """Returns date profile statistics for columns whose name contains 'date'. else returns None"""
    if "date" not in str(s.name).lower():
        return None
    dates=pd.to_datetime(s, errors="coerce", format="mixed")
    if dates.notna().sum() == 0:
        return None
    return {
        "min_date": str(dates.min().date()),
        "max_date": str(dates.max().date()),
        "future_count": int((dates > pd.Timestamp.today()).sum()),
        "unparseable_count": int(s.notna().sum() - dates.notna().sum()),
    }

def profile(df: pd.DataFrame, name: str) -> dict:
    """Return a data quality summary for a DataFrame."""
    columns = {}
    for col in df.columns:
        info = {
            "null_count": int(df[col].isna().sum()),
            "null_pct": round(df[col].isna().mean() * 100, 2) if len(df) else 0.0,
        }
        num = numeric_profile(df[col])
        if num:
            info["numeric"] = num
        dt = date_profile(df[col])
        if dt:
            info["date"] = dt
        columns[col] = info

    return {
        "name": name,
        "row_count": len(df),
        "column_count": len(df.columns),
        "duplicate_rows": int(df.duplicated().sum()),
        "columns": columns,
    }

if __name__ == "__main__":
    report = {}
    for name in ["transactions", "stores", "products"]:
        df = pd.read_csv(RAW_DIR/f"{name}.csv", dtype=str)
        report[name] = profile(df, name)

    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(OUTPUT_DIR/"profiling_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"{OUTPUT_DIR/'profiling_report.json'}")
