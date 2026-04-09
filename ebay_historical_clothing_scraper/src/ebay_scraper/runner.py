from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from .clothing_catalog import next_query_batch
from .config import load_settings
from .ebay_client import EbayAccessError, EbayClient, EbayListing
from .storage import ListingStorage


def run_once() -> dict:
    settings = load_settings()
    client = EbayClient(settings)
    storage = ListingStorage(settings.db_path)

    if settings.clothing_csv is not None:
        queries, catalog_start, catalog_next = next_query_batch(
            settings.clothing_csv,
            settings.clothing_cursor_path,
            settings.clothing_items_per_run,
        )
        if not queries:
            queries = settings.queries
            catalog_start = -1
            catalog_next = -1
    else:
        queries = settings.queries
        catalog_start = -1
        catalog_next = -1

    total_fetched = 0
    all_listings: list[EbayListing] = []
    try:
        for query in queries:
            listings = list(client.fetch_sold_listings(query=query))
            total_fetched += len(listings)
            all_listings.extend(listings)

        inserted = storage.save_listings(all_listings)
        csv_path = storage.export_all_to_csv(settings.export_dir)
        cleaner_script = (
            Path(__file__).resolve().parents[3] / "scripts" / "clean_ebay_exports.py"
        )
        subprocess.run(
            [sys.executable, str(cleaner_script)],
            check=True,
        )
        total_stored = total_fetched - inserted
        return {
            "queries": queries,
            "clothing_catalog": bool(settings.clothing_csv),
            "clothing_catalog_start_index": catalog_start,
            "clothing_catalog_next_cursor": catalog_next,
            "fetched": total_fetched,
            "inserted_new": inserted,
            "duplicates_ignored": total_stored,
            "csv_path": str(csv_path),
            "db_path": str(settings.db_path),
        }
    finally:
        storage.close()


if __name__ == "__main__":
    try:
        result = run_once()
    except EbayAccessError as err:
        print(err, file=sys.stderr)
        raise SystemExit(1) from err
    print("Run complete")
    if result.get("clothing_catalog"):
        print(
            f"Clothing catalog batch "
            f"(start_idx={result['clothing_catalog_start_index']}, "
            f"next_cursor={result['clothing_catalog_next_cursor']})"
        )
    print(f"Queries: {', '.join(result['queries'])}")
    print(f"Fetched: {result['fetched']}")
    print(f"Inserted new: {result['inserted_new']}")
    print(f"Duplicates ignored: {result['duplicates_ignored']}")
    print(f"Database: {result['db_path']}")
    print(f"CSV export: {result['csv_path']}")
