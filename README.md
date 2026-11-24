# Mindex Data Engineering Challenge: Retail Sales Pipeline

This pipeline takes three raw CSV exports from legacy retail systems, profiles them, cleans them, loads them into a SQLite star schema, and answers five business questions.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python scripts/seed_data.py      # (re)generate data/raw/*.csv
python src/pipeline.py           # profile -> clean -> reconcile -> load -> analytics
python -m pytest                 # 16 tests
```

Everything the pipeline produces goes to `output/`:

| File | Contents |
|---|---|
| `profiling_report.json` | Data quality profile of each raw file |
| `data_quality_issues.json` | Every data quality issue I found, with counts and what I did about it |
| `warehouse.db` | The star schema, plus a quarantine table so removed records can be reviewed |
| `analytics.json` | Answers to the five business questions |

## Architecture

```
 data/raw/*.csv
      │  Everything is read in as text
      ▼
 ┌──────────────┐    output/profiling_report.json
 │ profiler.py  │──► row/column counts, nulls, duplicates,
 └──────────────┘    numeric + date stats
      │
      ▼
 ┌──────────────┐    output/data_quality_issues.json
 │ cleaner.py   │──► issue log (table below)
 └──────────────┘──► quarantine: removed rows + reason + raw values
      │
      ▼
 ┌──────────────┐
 │ reconcile    │──► raw rows = kept + quarantined, or the run fails
 └──────────────┘
      │
      ▼
 ┌──────────────┐    output/warehouse.db
 │ loader.py    │──► dim_date, dim_store, dim_product, fact_sales, quarantine
 └──────────────┘
      │
      ▼
 ┌──────────────┐    output/analytics.json
 │ analytics.py │──► 5 SQL queries
 └──────────────┘
```

`pipeline.py` runs every step. I put the date and money parsers in `utils.py` so the profiler and the cleaner use the same rules.

| Module | What it does |
|---|---|
| `src/utils.py` | `parse_dates` (3 formats) and `parse_money` (`$`, `,`), shared by profiler and cleaner |
| `src/profiler.py` | `profile(df, name) -> dict` |
| `src/cleaner.py` | One cleaning function per file, plus a `CleaningReport` that records every issue and every removed row |
| `src/loader.py` | Builds and loads the star schema (full refresh, safe to rerun) |
| `src/analytics.py` | The five business questions as SQL |
| `src/pipeline.py` | Runs everything end to end, with logging and a row-count reconciliation |

## Data quality findings

The cleaner logs every fix as it makes it, so the counts below come straight from `output/data_quality_issues.json` and can't drift from the code.

I tried not to throw data away. If a row would make the numbers wrong, like a duplicate sale or a $0 void, I pulled it out and put it in a quarantine table with the reason, its line number in the CSV, and its original values. Everything else I kept and either fixed or flagged. I think that's safer than dropping rows, because anyone can go back and review what was removed.

| Issue | File | Count | Decision | Rationale |
|---|---|---|---|---|
| Duplicate rows | transactions | 15 | Quarantined, kept the first copy | Same transaction ID and values. Counting both would double-count revenue |
| Three date formats: `YYYY-MM-DD`, `MM/DD/YYYY`, `DD-MM-YYYY` | transactions | 20 | Parsed each format explicitly, based on the separator | Generic parsing read `01-04-2026` as Jan 4 instead of Apr 1. Values like `24-03` and `03/19` show which order each separator uses |
| `$` in `total_amount` | transactions | 25 | Stripped the `$` and converted to a number | Formatting only. The amounts are fine |
| Zero quantity and $0 total | transactions | 5 | Quarantined | Consecutive IDs (TXN10251–10255) at $0 look like voids or tests. They'd inflate transaction counts and pull down averages |
| Null `customer_id` | transactions | 40 | Kept, flagged `is_guest` | Guest checkouts are real sales. They're only left out of the customer question |
| Negative quantity/amount | transactions | 30 | Kept, flagged `is_return` | Returns should reduce net revenue |
| `store_id` not in stores.csv (S016–S019) | transactions | 5 | Kept, added placeholder `dim_store` rows (`is_inferred = 1`) | Dropping them would quietly lose real revenue. Placeholders keep totals right and are easy to filter out |
| `product_id` not in products.csv (P031, P032) | transactions | 3 | Kept, added placeholder `dim_product` rows (`is_inferred = 1`) | Same reason |
| Duplicate `store_id` S007 with a reworded name | stores | 1 | Quarantined the second row | Same city, zip, and open date. A dimension needs one row per store or joins will duplicate sales |
| Null `region` (S013, S014, both in OR) | stores | 2 | Filled from the US Census region for the state (OR → West) | Matches how the other stores are already labeled (NY Northeast, MN Midwest, TX South, WA West) |
| Duplicate row for P012 | products | 1 | Quarantined | Bad extract |
| P005 listed at two prices (141.61 and 150.11) | products | 1 | `list_price` = 141.61, quarantined the other row | All 19 P005 sales were charged 141.61. 150.11 looks like a price change that hadn't kicked in yet |
| `unit_price` = 0 for P027 | products | 1 | Replaced with the price its sales were actually charged | A $0 list price doesn't make sense for a product that sold |
| Null `category` | products | 5 | Filled with `Unknown` | Keeps that revenue in the category question instead of losing it |

**Result:** 505 → 482 transactions loaded and 26 rows quarantined. 19 stores (15 real + 4 placeholders), 32 products (30 + 2 placeholders), dates from 2026-03-05 to 2026-06-01.

### On the seed script

The seed script lists the issues it injects in its docstrings. I didn't realize that until after I'd profiled and explored the data, and by then I'd found the issues in the table above on my own. Reading it afterwards showed me three I'd missed. I fixed those, and they're listed here:

| Issue | File | Count | Decision | Rationale |
|---|---|---|---|---|
| `total_amount` ≠ `quantity × unit_price` | transactions | 20 | Kept, flagged `is_price_mismatch` | `total_amount` is what the customer was actually charged (looks like unrecorded discounts). "Correcting" it would invent revenue |
| Dated after the extract date (2026-06-02) | transactions | 3 | Quarantined | A sale can't happen after the data was exported. Keeping them would also shift the "last 30 days" window |
| Malformed zip `0938` (S003) | stores | 1 | Set to NULL | It's 4 digits. Greece, NY zips start with `14`, so I'd originally zero-padded it to a zip that doesn't exist |

The extract date (`EXTRACT_DATE` in `cleaner.py`) comes from the seed script.

**On AI use:** I used an AI assistant to help draft and review code. 

### Profiler notes

- Files are read as text, so the profiler sees the raw values. A column only gets numeric stats if at least 80% of its values are numbers. `non_numeric_count` then shows the dirty ones, like `$615.85`.
- Date stats use the same `parse_dates` as the cleaner. `future_count` is measured against today's date, as the challenge asks, so it shows 0. The cleaner catches the post-extract rows instead.
- An ID column that happens to look numeric will get numeric stats. That's harmless, but worth knowing.

## Schema

The full DDL is in `src/loader.py`.

```
                 ┌─────────────┐
                 │  dim_date   │
                 │ date_key PK │
                 └──────┬──────┘
                        │
┌─────────────┐  ┌──────┴────────┐  ┌───────────────┐
│  dim_store  ├──┤  fact_sales   ├──┤  dim_product  │
│ store_key PK│  │ transaction_id│  │ product_key PK│
└─────────────┘  └───────────────┘  └───────────────┘

quarantine  (audit table, not part of the star)
```

| Table | Grain | Key columns |
|---|---|---|
| `dim_date` | One row per day from the first to the last transaction (89 rows), including days with no sales | `date_key` (YYYYMMDD), `full_date`, `year`, `quarter`, `month`, `month_name`, `year_month`, `day_of_week`, `day_name`, `week_of_year`, `is_weekend` |
| `dim_store` | One row per store (19) | `store_key` (surrogate), `store_id`, name, city, state, `zip_code` (text), `region`, `opened_date`, `is_inferred` |
| `dim_product` | One row per product (32) | `product_key` (surrogate), `product_id`, name, `category`, `list_price`, `supplier_id`, `is_inferred` |
| `fact_sales` | One row per transaction (482) | `transaction_id` PK, FKs to the three dimensions, `customer_id`, `quantity`, `unit_price`, `total_amount`, `is_return`, `is_guest`, `is_price_mismatch` |
| `quarantine` | One row per removed source row (26) | `source_file`, `source_row` (CSV line number), `reason`, `raw_record` (original values as JSON) |

### Key modeling decisions

- **Surrogate keys.** I kept `store_key` and `product_key` separate from the business IDs, so placeholder rows and any future history tracking don't depend on the source IDs. `date_key` is a readable YYYYMMDD number.
- **Products with more than one price.** Revenue always comes from what each sale was actually charged (`fact_sales.unit_price` and `total_amount`), so a catalog price change never rewrites history. `dim_product.list_price` is reference only. In production I'd make `dim_product` a Type 2 slowly changing dimension.
- **Returns** stay in `fact_sales` as negative `quantity` and `total_amount`, with `is_return = 1`. A plain `SUM(total_amount)` gives net revenue, and returns are easy to filter out when a question asks for that.
- **Guests** have a NULL `customer_id` and `is_guest = 1`.
- **Excluded records:** 15 duplicate transactions, 5 zero-quantity voids, 3 post-extract transactions, and the duplicate S007, P012, and stale P005 rows. All 26 are in `quarantine` and can be reviewed or reprocessed.
- **Reconciliation.** Before loading, the pipeline checks that raw rows = kept + quarantined for every file (505 = 482 + 23 for transactions). If any row is unaccounted for, it logs an error and stops. I used a custom exception instead of `assert`, because `python -O` skips asserts.
- **Integrity.** Foreign keys are declared and enforced, and the loader raises an error if any sale doesn't match a dimension row.
- **Full refresh.** Each run drops and rebuilds the warehouse from the raw files, so it's safe to rerun. I'd rather this be incremental in production. See *Productionizing*.

## Analytics

I answered all five questions in SQL (`src/analytics.py`). The data is already in a star schema, the queries read like the questions, and they'd run almost unchanged on Snowflake, BigQuery, or Postgres. Revenue is always net, so returns reduce it.

### 1. Top 5 stores by net revenue, last 30 days of data

I used the 30 days ending on the last date in the data (2026-05-03 to 2026-06-01), not today's date. Today would return nothing.

| Store | Region | Net revenue | Transactions |
|---|---|---|---|
| S011 Southpark Meadows | South | $7,342.43 | 20 |
| S008 Galleria at Crystal Run | Northeast | $6,770.08 | 19 |
| S001 Eastview Mall | Northeast | $6,179.91 | 13 |
| S014 Lloyd Center | West | $4,979.12 | 12 |
| S012 The Domain | South | $4,938.18 | 14 |

### 2. Month-over-month revenue change by category

| Category | Apr vs Mar | May vs Apr |
|---|---|---|
| Apparel | +12.1% | +34.3% |
| Electronics | +91.8% | +61.1% |
| Food & Beverage | +403.2% | +31.0% |
| Home & Garden | +28.6% | +64.7% |
| Office Supplies | +7.3% | −38.1% |
| Unknown (null category) | +22.8% | +47.3% |

One caveat: the data runs from March 5 to June 1, so March (27 days) and June (1 day) are partial months. The query flags them with `days_in_data` and `is_partial_month`. June shows a drop of around 96–100%, but that's because there's only one day of data, not a real decline. The query also builds every category × month combination, so a month with no sales shows as 0 instead of being skipped.

### 3. Return rate by store (flag > 10%)

Three stores are over 10%:

| Store | Returns / transactions | Rate |
|---|---|---|
| S015 Alderwood Mall | 4 / 26 | 15.4% ⚠ |
| S006 Lakeside Shopping Ctr | 4 / 33 | 12.1% ⚠ |
| S008 Galleria at Crystal Run | 5 / 44 | 11.4% ⚠ |

Every other store is at or below 8.1%. The full list is in `analytics.json`.

### 4. Average transaction value by region (returns excluded)

| Region | Avg transaction | Transactions |
|---|---|---|
| Northeast | $387.72 | 167 |
| South | $382.85 | 84 |
| Midwest | $375.82 | 46 |
| West | $364.02 | 150 |
| Unknown (placeholder stores) | $253.14 | 5 |

### 5. Top 10 customers by lifetime spend (guests excluded)

Lifetime spend is net of returns. Order count and average order value only count purchases, so a return isn't counted as an order.

| Customer | Lifetime spend | Orders | Avg order value |
|---|---|---|---|
| CUST0213 | $3,077.96 | 4 | $769.49 |
| CUST0170 | $2,854.52 | 5 | $570.90 |
| CUST0287 | $2,825.70 | 4 | $706.42 |
| CUST0060 | $2,414.60 | 2 | $1,207.30 |
| CUST0142 | $2,284.70 | 3 | $761.57 |
| CUST0255 | $2,181.57 | 5 | $436.31 |
| CUST0186 | $1,906.56 | 4 | $476.64 |
| CUST0085 | $1,846.65 | 4 | $461.66 |
| CUST0084 | $1,831.43 | 4 | $457.86 |
| CUST0118 | $1,655.79 | 2 | $827.89 |

## Tests

`python -m pytest` runs 16 tests in `tests/test_pipeline.py`:

- **Profiler (4):** known counts, nulls, duplicates, and numeric stats; an empty DataFrame; an all-null column; mixed-format dates.
- **Cleaning (5):** date parsing by separator (including the DD-MM trap), money parsing, transaction dedupe, flags and quarantine contents, store zip/region/duplicate handling, and the multi-price and unknown-ID rules.
- **Analytics (5):** six hand-picked transactions loaded through the real cleaner and loader into a temporary SQLite database. Each query is checked against answers I worked out by hand, which are written out in the fixture's docstring.
- **Reconciliation (2):** every raw row is either kept or quarantined, and a silently dropped row fails the run.

## Productionizing

- **Orchestration.** I'd run each step as its own task in Airflow or Dagster (extract → profile → clean → reconcile → load → analytics → checks), with retries and alerts. The functions are already separate, so each one maps to a task.
- **Incremental loads.** I'd replace the full refresh with:
  - a `load_log` table that records each batch (file name, checksum, row counts, max `transaction_date`) as a high-water mark, so a file is never loaded twice;
  - a staging table for each batch, then a `MERGE` into the warehouse. Matched rows that changed get updated, new rows get inserted, and unchanged rows are skipped. That way a corrected transaction in a later file actually gets applied instead of ignored. I'd add `batch_id` and `updated_at` to every table so I can see what changed and when;
  - for stores, a straight `MERGE` that updates in place (SCD Type 1). For products, a `MERGE` that expires the current row and inserts a new version when the price changes (SCD Type 2), so older sales keep the price that was valid at the time;
  - an append-only quarantine tagged with `batch_id`, and placeholder stores and products that get filled in when the real record shows up.
- **Monitoring and data quality.** I'd turn the issue log and the reconciliation check into automated checks (Great Expectations, dbt tests, or plain SQL) with thresholds. For example, fail the run if more than 5% of rows are quarantined, alert on new unknown IDs, and track return rates and row counts over time. Run metrics (rows in, out, quarantined, duration) would go to a dashboard.
- **Configuration.** `EXTRACT_DATE`, thresholds, and paths should live in config. In a real feed, the extract date would come from file metadata, not a constant.
- **Storage.** With more tables, I'd move the SQL into dbt models (one per file, with dbt handling dependencies) on a warehouse like Snowflake, BigQuery, or Postgres. dbt's incremental models use this same `MERGE` approach, and snapshots handle the SCD Type 2 products.

## Project layout

```
├── README.md
├── requirements.txt
├── pytest.ini
├── data/raw/                 # source CSVs (unmodified)
├── scripts/seed_data.py
├── output/                   # generated by the pipeline
├── src/
│   ├── utils.py
│   ├── profiler.py
│   ├── cleaner.py
│   ├── loader.py
│   ├── analytics.py
│   └── pipeline.py
└── tests/
    └── test_pipeline.py
```