"""Shared parsing helpers used by the profiler and the cleaner."""

import pandas as pd


def parse_dates(s: pd.Series) -> pd.Series:
    """Parse a column holding three date formats, using the separator to decide.

    YYYY-MM-DD (ISO), MM/DD/YYYY (slashes), DD-MM-YYYY (dashes, year last).
    """
    s = s.str.strip()
    out = pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")
    slash = s.str.match(r"^\d{2}/\d{2}/\d{4}$", na=False)
    dmy = s.str.match(r"^\d{2}-\d{2}-\d{4}$", na=False)
    out[slash] = pd.to_datetime(s[slash], format="%m/%d/%Y", errors="coerce")
    out[dmy] = pd.to_datetime(s[dmy], format="%d-%m-%Y", errors="coerce")
    return out


def parse_money(s: pd.Series) -> pd.Series:
    """Strip '$' and ',' and convert to float."""
    return pd.to_numeric(
        s.astype("string").str.replace(r"[$,]", "", regex=True).str.strip(),
        errors="coerce",
    )