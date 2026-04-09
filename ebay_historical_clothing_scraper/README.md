# eBay Historical Clothing Scraper

Python project that collects sold/completed eBay clothing listings every day and stores them locally.

## What it collects

- Listing title
- Sold price text and parsed numeric value (when possible)
- Shipping text
- Item condition
- Sold date text
- Listing URL
- eBay item ID
- Search query used
- Collection timestamp

## Project structure

- `src/ebay_scraper/config.py` - environment configuration
- `src/ebay_scraper/ebay_client.py` - eBay sold listing scraper
- `src/ebay_scraper/storage.py` - SQLite storage and CSV exports
- `src/ebay_scraper/runner.py` - run once
- `src/ebay_scraper/schedule_daily.py` - run automatically once per day

## Setup

1. Create and activate virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Optional: copy `.env.example` to `.env` and customize values.

## Run once

From project root:

```bash
PYTHONPATH=src python -m ebay_scraper.runner
```

## Run every day (in-process scheduler)

```bash
PYTHONPATH=src python -m ebay_scraper.schedule_daily
```

If `clothing.csv` is present in the project root (columns `Item`, `Brand`, …), each scheduled run uses the next **5** rows (configurable with `CLOTHING_ITEMS_PER_RUN`) as search queries, built as `"{Brand} {Item}"`. Progress is stored in `data/clothing_scrape_cursor.txt` so the next run continues through the file and wraps. Set `EBAY_USE_CLOTHING_CATALOG=0` in `.env` to keep using only `EBAY_QUERIES`.

Default schedule is `02:00` (24-hour local time). Change it in `.env` with:

- `SCHEDULE_HOUR=2`
- `SCHEDULE_MINUTE=0`

## Run every day with cron (macOS/Linux)

1. Get absolute paths for your Python and project directory:

```bash
which python
pwd
```

2. Open crontab:

```bash
crontab -e
```

3. Add a daily job (example: 2:00 AM):

```cron
0 14 * * * cd /Users/katmalneva/MSDS/MLops/ebay_historical_clothing_scraper && PYTHONPATH=src /opt/anaconda3/envs/MLops/bin/python -m ebay_scraper.runner >> data/cron.log 2>&1
```

This command appends logs to `data/cron.log`.

## Output data

- SQLite DB: `data/ebay_historical.db`
- CSV snapshots: `data/exports/ebay_historical_YYYYMMDD_HHMMSS.csv`

## Build cleaned modeling dataset

From project root, run:

```bash
python scripts/clean_ebay_exports.py
```

This scans scraper CSV files, uses `data/brands.csv` to match brand names, extracts `item_type`, normalizes `condition`, and keeps numeric `price`.

Outputs:

- Cleaned CSV: `data/processed/ebay_historical_cleaned.csv`
- Cleaned Parquet: `data/processed/ebay_historical_cleaned.parquet`
- Cleaned SQLite DB: `data/processed/ebay_cleaned.db` (table: `ebay_historical_cleaned`)

## Notes

- This scraper targets eBay sold/completed listing search result pages.
- HTML structure can change over time; adjust parsing selectors in `ebay_client.py` if needed.
- Respect eBay terms and use reasonable request rates.
