"""End-to-end pipeline: profile -> clean -> load -> analytics.

Usage:
    python src/pipeline.py
"""

import json
import logging
from pathlib import Path

import pandas as pd

from analytics import write_analytics
from cleaner import CleaningReport, clean_all
from loader import build_warehouse
from profiler import profile

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
OUTPUT_DIR = ROOT / "output"
SOURCES = ["transactions", "stores", "products"]

log = logging.getLogger("pipeline")


class ReconciliationError(Exception):
    """Raised when rows go missing between the raw files and the warehouse."""


def write_json(obj, path: Path) -> None:
    with open(path, "w") as f:
        json.dump(obj, f, indent=2, default=str)


def reconcile(raw: dict[str, pd.DataFrame], cleaned: dict[str, pd.DataFrame],
              report: CleaningReport) -> dict[str, dict]:
    """Check that every raw row was either kept or quarantined: raw = kept + quarantined.

    Placeholder dimension rows (is_inferred) are added by the pipeline, not read
    from the source, so they are not counted as kept. Raises ReconciliationError
    (not `assert`, which `python -O` silently skips).
    """
    quarantined = report.quarantine_df()["source_file"].value_counts()
    results, failures = {}, []
    for name, raw_df in raw.items():
        df = cleaned[name]
        kept = int((~df["is_inferred"]).sum()) if "is_inferred" in df else len(df)
        q = int(quarantined.get(name, 0))
        results[name] = {"raw": len(raw_df), "kept": kept, "quarantined": q}
        if len(raw_df) != kept + q:
            failures.append(f"{name}: raw {len(raw_df)} != kept {kept} + quarantined {q}")
    if failures:
        raise ReconciliationError("; ".join(failures))
    return results


def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)

##Extracting the data
    raw = {name: pd.read_csv(RAW_DIR / f"{name}.csv", dtype=str) for name in SOURCES}
    for name, df in raw.items():
        log.info("extract  %-12s %4d rows", name, len(df))

##Running the profiler
    write_json({name: profile(df, name) for name, df in raw.items()},
               OUTPUT_DIR / "profiling_report.json")
    log.info("profile  wrote output/profiling_report.json")

##Running the cleaner
    t, s, p, report = clean_all(raw["transactions"], raw["stores"], raw["products"])
    write_json(report.issues, OUTPUT_DIR / "data_quality_issues.json")
    log.info("clean    %d issues logged, %d rows quarantined -> output/data_quality_issues.json",
             len(report.issues), len(report.quarantine))

##Ensure that no record is silently dropper, it either kept or quarrantined
    for name, r in reconcile(raw, {"transactions": t, "stores": s, "products": p},
                             report).items():
        log.info("reconcile %-11s raw %4d = kept %4d + quarantined %3d",
                 name, r["raw"], r["kept"], r["quarantined"])

    counts = build_warehouse(t, s, p, OUTPUT_DIR / "warehouse.db",
                             quarantine=report.quarantine_df())
    for table, n in counts.items():
        log.info("load     %-12s %4d rows", table, n)

##Running the Analytics file.
    results = write_analytics(OUTPUT_DIR / "warehouse.db", OUTPUT_DIR / "analytics.json")
    log.info("analyze  %d questions answered -> output/analytics.json", len(results))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s",
                        datefmt="%H:%M:%S")
    try:
        main()
    except ReconciliationError as exc:
        log.error("reconciliation failed: %s", exc)
        raise SystemExit(1)